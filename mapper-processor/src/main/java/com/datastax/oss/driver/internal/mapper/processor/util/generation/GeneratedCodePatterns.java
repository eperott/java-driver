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
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
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
      throw new UnsupportedOperationException("TODO handle collections of UDTs");
    }
  }
}
