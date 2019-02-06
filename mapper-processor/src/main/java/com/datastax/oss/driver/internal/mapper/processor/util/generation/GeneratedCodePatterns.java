/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.mapper.processor.util.generation;

import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.core.data.SettableByName;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import java.beans.Introspector;
import java.util.List;
import java.util.Map;
import javax.lang.model.element.VariableElement;

/** A collection of recurring patterns in our generated sources. */
public class GeneratedCodePatterns {

  /**
   * The names of the primitive getters/setters on {@link GettableByName} and {@link
   * SettableByName}.
   */
  public static final Map<TypeName, String> PRIMITIVE_ACCESSORS =
      ImmutableMap.<TypeName, String>builder()
          .put(TypeName.BOOLEAN, "Boolean")
          .put(TypeName.BYTE, "Byte")
          .put(TypeName.DOUBLE, "Double")
          .put(TypeName.FLOAT, "Float")
          .put(TypeName.INT, "Int")
          .put(TypeName.LONG, "Long")
          .build();

  /**
   * Treats a list of method parameters as bind variables in a query.
   *
   * <p>The generated code assumes that a {@code BoundStatementBuilder boundStatementBuilder} local
   * variable already exists.
   */
  public static void bindParameters(
      List<? extends VariableElement> parameters,
      MethodSpec.Builder methodBuilder,
      BindableHandlingSharedCode enclosingClass,
      ProcessorContext context) {

    for (VariableElement parameter : parameters) {
      String parameterName = parameter.getSimpleName().toString();
      PropertyType type = PropertyType.parse(parameter.asType(), context);
      setValue(
          parameterName,
          type,
          CodeBlock.of("$L", parameterName),
          "boundStatementBuilder",
          methodBuilder,
          enclosingClass);
    }
  }

  /**
   * Generates the code to set a value on a {@link SettableByName} instance.
   *
   * <p>Example:
   *
   * <pre>{@code
   * target = target.set("id", entity.getId(), UUID.class);
   * }</pre>
   *
   * @param cqlName the CQL name to set ({@code "id"})
   * @param type the type of the value ({@code UUID})
   * @param valueExtractor the code snippet to extract the value ({@code entity.getId()}
   * @param targetName the name of the target {@link SettableByName} instance ({@code target})
   * @param methodBuilder where to add the code
   * @param enclosingClass a reference to the parent generator (in case type constants or entity
   *     helpers are needed)
   */
  public static void setValue(
      String cqlName,
      PropertyType type,
      CodeBlock valueExtractor,
      String targetName,
      MethodSpec.Builder methodBuilder,
      BindableHandlingSharedCode enclosingClass) {

    methodBuilder.addComment("$L:", cqlName);

    if (type instanceof PropertyType.Simple) {
      TypeName typeName = ((PropertyType.Simple) type).typeName;
      String primitiveAccessor = GeneratedCodePatterns.PRIMITIVE_ACCESSORS.get(typeName);
      if (primitiveAccessor != null) {
        // Primitive type: use dedicated setter, since it is optimized to avoid boxing.
        //     target = target.setInt("length", entity.getLength());
        methodBuilder.addStatement(
            "$1L = $1L.set$2L($3S, $4L)", targetName, primitiveAccessor, cqlName, valueExtractor);
      } else if (typeName instanceof ClassName) {
        // Unparameterized class: use the generic, class-based setter.
        //     target = target.set("id", entity.getId(), UUID.class);
        methodBuilder.addStatement(
            "$1L = $1L.set($2S, $3L, $4T.class)", targetName, cqlName, valueExtractor, typeName);
      } else {
        // Parameterized type: create a constant and use the GenericType-based setter.
        //     private static final GenericType<List<String>> GENERIC_TYPE =
        //         new GenericType<List<String>>(){};
        //     target = target.set("names", entity.getNames(), GENERIC_TYPE);
        // Note that lists, sets and maps of unparameterized classes also fall under that
        // category. Their setter creates a GenericType under the hood, so there's no performance
        // advantage in calling them instead of the generic set().
        methodBuilder.addStatement(
            "$1L = $1L.set($2S, $3L, $4L)",
            targetName,
            cqlName,
            valueExtractor,
            enclosingClass.addGenericTypeConstant(typeName));
      }
    } else if (type instanceof PropertyType.SingleEntity) {
      ClassName entityName = ((PropertyType.SingleEntity) type).entityName;
      // Other entity class: the CQL column is a mapped UDT. Example of generated code:
      //     Dimensions value = entity.getDimensions();
      //     if (value != null) {
      //       UserDefinedType udtType = (UserDefinedType) target.getType("dimensions");
      //       UdtValue udtValue = udtType.newValue();
      //       dimensionsHelper.set(value, udtValue);
      //       target = target.setUdtValue("dimensions", udtValue);
      //     }

      // Generate unique names for our temporary variables. Note that they are local so we don't
      // strictly need class-wide uniqueness, but it's simpler to reuse the NameIndex
      String udtTypeName = enclosingClass.getNameIndex().uniqueField("udtType");
      String udtValueName = enclosingClass.getNameIndex().uniqueField("udtValue");
      String valueName = enclosingClass.getNameIndex().uniqueField("value");

      methodBuilder
          .addStatement("$T $L = $L", entityName, valueName, valueExtractor)
          .beginControlFlow("if ($L != null)", valueName)
          .addStatement(
              "$1T $2L = ($1T) $3L.getType($4S)",
              UserDefinedType.class,
              udtTypeName,
              targetName,
              cqlName)
          .addStatement("$T $L = $L.newValue()", UdtValue.class, udtValueName, udtTypeName);
      String childHelper = enclosingClass.addEntityHelperField(entityName);
      methodBuilder
          .addStatement("$L.set($L, $L)", childHelper, valueName, udtValueName)
          .addStatement("$1L = $1L.setUdtValue($2S, $3L)", targetName, cqlName, udtValueName)
          .endControlFlow();
    } else {
      String valueName = enclosingClass.getNameIndex().uniqueField("value");
      methodBuilder
          .addStatement("$T $L = $L", type.asTypeName(), valueName, valueExtractor)
          .beginControlFlow("if ($L != null)", valueName);

      String convertedValueName = enclosingClass.getNameIndex().uniqueField("convertedValue");
      CodeBlock currentCqlType = CodeBlock.of("$L.getType($S)", targetName, cqlName);
      CodeBlock.Builder udtTypesBuilder = CodeBlock.builder();
      CodeBlock.Builder conversionCodeBuilder = CodeBlock.builder();
      convertEntityCollection(
          valueName,
          convertedValueName,
          type,
          currentCqlType,
          udtTypesBuilder,
          conversionCodeBuilder,
          enclosingClass);

      methodBuilder
          .addCode(udtTypesBuilder.build())
          .addCode(conversionCodeBuilder.build())
          .addStatement(
              "$1L = $1L.set($2S, $3L, $4L)",
              targetName,
              cqlName,
              convertedValueName,
              enclosingClass.addGenericTypeConstant(type.asConvertedTypeName()))
          .endControlFlow();
    }
  }

  /**
   * Generates the code to convert a collection of mapped entities.
   *
   * @param objectName the name of the local variable containing the value to convert.
   * @param convertedObjectName the name of the local variable that must be created to store the
   *     converted value.
   * @param type the type of the value.
   * @param currentCqlType a code snippet to extract the CQL type corresponding to {@code type}.
   * @param udtTypesBuilder the code block that comes before the conversion. It creates local
   *     variables that extract the required {@link UserDefinedType} instances from the target
   *     container.
   * @param conversionBuilder the code block to generate the conversion code into.
   */
  private static void convertEntityCollection(
      String objectName,
      String convertedObjectName,
      PropertyType type,
      CodeBlock currentCqlType,
      CodeBlock.Builder udtTypesBuilder,
      CodeBlock.Builder conversionBuilder,
      BindableHandlingSharedCode enclosingClass) {

    if (type instanceof PropertyType.SingleEntity) {
      ClassName entityName = ((PropertyType.SingleEntity) type).entityName;
      String udtTypeName =
          enclosingClass
              .getNameIndex()
              .uniqueField(Introspector.decapitalize(entityName.simpleName()) + "UdtType");
      udtTypesBuilder.addStatement(
          "$1T $2L = ($1T) $3L", UserDefinedType.class, udtTypeName, currentCqlType);

      String entityHelperName = enclosingClass.addEntityHelperField(entityName);
      conversionBuilder
          .addStatement("$T $L = $L.newValue()", UdtValue.class, convertedObjectName, udtTypeName)
          .addStatement("$L.set($L, $L)", entityHelperName, objectName, convertedObjectName);
    } else if (type instanceof PropertyType.EntityList) {
      PropertyType elementType = ((PropertyType.EntityList) type).elementType;
      TypeName convertedTypeName = type.asConvertedTypeName();
      conversionBuilder.addStatement(
          "$T $L = $T.newArrayListWithExpectedSize($L.size())",
          convertedTypeName,
          convertedObjectName,
          Lists.class,
          objectName);
      String loopVariableName = enclosingClass.getNameIndex().uniqueField("element");
      conversionBuilder.beginControlFlow(
          "for ($T $L: $L)", elementType.asTypeName(), loopVariableName, objectName);
      String convertedElementName = enclosingClass.getNameIndex().uniqueField("convertedElement");
      convertEntityCollection(
          loopVariableName,
          convertedElementName,
          elementType,
          CodeBlock.of("(($T) $L).getElementType()", ListType.class, currentCqlType),
          udtTypesBuilder,
          conversionBuilder,
          enclosingClass);
      conversionBuilder
          .addStatement("$L.add($L)", convertedObjectName, convertedElementName)
          .endControlFlow();
    } else if (type instanceof PropertyType.EntitySet) {
      PropertyType elementType = ((PropertyType.EntitySet) type).elementType;
      TypeName convertedTypeName = type.asConvertedTypeName();
      conversionBuilder.addStatement(
          "$T $L = $T.newLinkedHashSetWithExpectedSize($L.size())",
          convertedTypeName,
          convertedObjectName,
          Sets.class,
          objectName);
      String loopVariableName = enclosingClass.getNameIndex().uniqueField("element");
      conversionBuilder.beginControlFlow(
          "for ($T $L: $L)", elementType.asTypeName(), loopVariableName, objectName);
      String convertedElementName = enclosingClass.getNameIndex().uniqueField("convertedElement");
      convertEntityCollection(
          loopVariableName,
          convertedElementName,
          elementType,
          CodeBlock.of("(($T) $L).getElementType()", ListType.class, currentCqlType),
          udtTypesBuilder,
          conversionBuilder,
          enclosingClass);
      conversionBuilder
          .addStatement("$L.add($L)", convertedObjectName, convertedElementName)
          .endControlFlow();
    } else if (type instanceof PropertyType.EntityMap) {
      PropertyType keyType = ((PropertyType.EntityMap) type).keyType;
      PropertyType valueType = ((PropertyType.EntityMap) type).valueType;
      TypeName convertedTypeName = type.asConvertedTypeName();
      conversionBuilder.addStatement(
          "$T $L = $T.newLinkedHashMapWithExpectedSize($L.size())",
          convertedTypeName,
          convertedObjectName,
          Maps.class,
          objectName);
      String loopVariableName = enclosingClass.getNameIndex().uniqueField("entry");
      conversionBuilder.beginControlFlow(
          "for ($T $L: $L.entrySet())",
          ParameterizedTypeName.get(
              ClassName.get(Map.Entry.class), keyType.asTypeName(), valueType.asTypeName()),
          loopVariableName,
          objectName);
      String keyName = CodeBlock.of("$L.getKey()", loopVariableName).toString();
      String convertedKeyName;
      if (keyType instanceof PropertyType.Simple) {
        convertedKeyName = keyName; // no conversion, use the instance as-is
      } else {
        convertedKeyName = enclosingClass.getNameIndex().uniqueField("convertedKey");
        convertEntityCollection(
            keyName,
            convertedKeyName,
            keyType,
            CodeBlock.of("(($T) $L).getKeyType()", MapType.class, currentCqlType),
            udtTypesBuilder,
            conversionBuilder,
            enclosingClass);
      }
      String valueName = CodeBlock.of("$L.getValue()", loopVariableName).toString();
      String convertedValueName;
      if (valueType instanceof PropertyType.Simple) {
        convertedValueName = valueName;
      } else {
        convertedValueName = enclosingClass.getNameIndex().uniqueField("convertedValue");
        convertEntityCollection(
            valueName,
            convertedValueName,
            valueType,
            CodeBlock.of("(($T) $L).getValueType()", MapType.class, currentCqlType),
            udtTypesBuilder,
            conversionBuilder,
            enclosingClass);
      }
      conversionBuilder
          .addStatement("$L.put($L, $L)", convertedObjectName, convertedKeyName, convertedValueName)
          .endControlFlow();
    } else {
      throw new AssertionError("Unsupported type " + type.asTypeName());
    }
  }
}
