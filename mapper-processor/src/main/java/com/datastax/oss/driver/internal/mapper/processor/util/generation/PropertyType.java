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
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

/**
 * Wraps the declared type of an entity property (or DAO method parameter) that will be injected
 * into a {@link SettableByName}, or extracted from a {@link GettableByName}.
 *
 * <p>The goal is to detect if the type contains other mapped entities, that must be translated into
 * UDT values.
 */
public class PropertyType {

  public static PropertyType parse(TypeMirror typeMirror, ProcessorContext context) {
    if (typeMirror.getKind() == TypeKind.DECLARED) {
      DeclaredType declaredType = (DeclaredType) typeMirror;
      if (declaredType.asElement().getAnnotation(Entity.class) != null) {
        return new SingleEntity(declaredType);
      } else if (context.getClassUtils().isList(declaredType)) {
        PropertyType elementType = parse(declaredType.getTypeArguments().get(0), context);
        return (elementType instanceof Simple)
            ? new Simple(typeMirror)
            : new EntityList(elementType);
      } else if (context.getClassUtils().isSet(declaredType)) {
        PropertyType elementType = parse(declaredType.getTypeArguments().get(0), context);
        return (elementType instanceof Simple)
            ? new Simple(typeMirror)
            : new EntitySet(elementType);
      } else if (context.getClassUtils().isMap(declaredType)) {
        PropertyType keyType = parse(declaredType.getTypeArguments().get(0), context);
        PropertyType valueType = parse(declaredType.getTypeArguments().get(1), context);
        return (keyType instanceof Simple && valueType instanceof Simple)
            ? new Simple(typeMirror)
            : new EntityMap(keyType, valueType);
      }
    }
    return new Simple(typeMirror);
  }

  /**
   * A type that does not contain any mapped entity.
   *
   * <p>Note that it can still be a collection, for example {@code Map<String, List<Integer>>}.
   */
  public static class Simple extends PropertyType {
    public final TypeName typeName;

    public Simple(TypeMirror typeMirror) {
      this.typeName = ClassName.get(typeMirror);
    }
  }

  /** A mapped entity. */
  public static class SingleEntity extends PropertyType {
    public final ClassName entityName;

    public SingleEntity(DeclaredType declaredType) {
      this.entityName = (ClassName) TypeName.get(declaredType);
    }
  }

  /** A list of another non-simple type. */
  public static class EntityList extends PropertyType {
    public final PropertyType elementType;

    public EntityList(PropertyType elementType) {
      this.elementType = elementType;
    }
  }

  /** A set of another non-simple type. */
  public static class EntitySet extends PropertyType {
    public final PropertyType elementType;

    public EntitySet(PropertyType elementType) {
      this.elementType = elementType;
    }
  }

  /** A map where either the key type, the value type, or both, are non-simple types. */
  public static class EntityMap extends PropertyType {
    public final PropertyType keyType;
    public final PropertyType valueType;

    public EntityMap(PropertyType keyType, PropertyType valueType) {
      this.keyType = keyType;
      this.valueType = valueType;
    }
  }
}
