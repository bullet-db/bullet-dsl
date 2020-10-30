/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.dsl.converter;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.dsl.BulletDSLConfig;
import com.yahoo.bullet.dsl.BulletDSLException;
import com.yahoo.bullet.dsl.schema.BulletRecordField;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;
import javafx.util.Pair;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * POJOBulletRecordConverter is used to convert POJOs to {@link BulletRecord}.
 * <br><br>
 * If a schema is not specified, the POJOBulletRecordConverter constructed will convert all valid fields (with valid types)
 * and log a warning for each invalid field.
 * If a schema is specified, method names in addition to member names can be used in references.
 * <br><br>
 * Note, POJOBulletRecordConverter uses reflections and is relatively slow; specifying getters in the schema will
 * lead to slightly better performance. Furthermore, the converter only finds declared fields and methods and does not
 * look into superclasses or interfaces.
 */
@Slf4j
public class POJOBulletRecordConverter extends BulletRecordConverter {

    private static final List<Class> PRIMITIVES = Arrays.asList(Boolean.class, Integer.class, Long.class, Float.class, Double.class, String.class);
    private static final long serialVersionUID = 1542840952973181399L;

    // Exposed for testing
    @Getter(AccessLevel.PACKAGE)
    private Map<String, Pair<Method, Field>> accessors = new HashMap<>();
    private Class<?> pojoType;

    /**
     * Constructs a POJOBulletRecordConverter without a schema that finds all valid fields regardless of access
     * (protected, private, etc.) though not including inherited fields.
     *
     * @param type The POJO class type.
     * @throws BulletDSLException if there is an error creating the converter.
     */
    public POJOBulletRecordConverter(Class<?> type) throws BulletDSLException {
        this(type, null);
    }

    /**
     * Constructs a POJOBulletRecordConverter from a given schema.
     *
     * @param type The POJO class type.
     * @param schema A schema file that specifies the fields to extract and their types.
     * @throws BulletDSLException if there is an error creating the converter.
     */
    public POJOBulletRecordConverter(Class<?> type, String schema) throws BulletDSLException {
        super(null);
        Objects.requireNonNull(type);
        config.set(BulletDSLConfig.RECORD_CONVERTER_POJO_CLASS_NAME, type.getName());
        config.set(BulletDSLConfig.RECORD_CONVERTER_SCHEMA_FILE, schema);
        config.validate();
        build();
    }

    /**
     * Constructs a POJOBulletRecordConverter from a given configuration.
     *
     * @param bulletConfig The configuration that specifies the settings for a POJOBulletRecordConverter.
     * @throws BulletDSLException if there is an error creating the converter.
     */
    public POJOBulletRecordConverter(BulletConfig bulletConfig) throws BulletDSLException {
        super(bulletConfig);
        build();
    }

    @Override
    protected BulletRecordConverter build() throws BulletDSLException {
        super.build();
        try {
            String className = config.getAs(BulletDSLConfig.RECORD_CONVERTER_POJO_CLASS_NAME, String.class);
            pojoType = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new BulletDSLException("Could not find POJO class.", e);
        }
        if (schema != null) {
            initWithSchema();
        } else {
            initWithoutSchema();
        }
        return this;
    }

    /**
     * Helper function that sets the field/getter accessors for the POJO of this converter with a schema.
     */
    private void initWithSchema() throws BulletDSLException {
        for (BulletRecordField field : schema.getFields()) {
            String[] token = field.getToken();
            if (!setMethodAccessor(token, field) && !setFieldAccessor(token, field)) {
                throw new BulletDSLException("Accessor for field not found: " + field);
            }
        }
    }

    /**
     * Helper function that sets the field accessors for the POJO of this converter.
     */
    private void initWithoutSchema() {
        for (Field field : pojoType.getDeclaredFields()) {
            if (!field.isSynthetic()) {
                if (hasValidType(field)) {
                    field.setAccessible(true);
                    accessors.put(field.getName(), new Pair<>(null, field));
                } else {
                    log.warn("Ignoring field with unsupported type: " + field.getName());
                }
            }
        }
    }

    private boolean setMethodAccessor(String[] token, BulletRecordField field) throws BulletDSLException {
        try {
            Method m = pojoType.getDeclaredMethod(token[0]);
            if (token.length == 1) {
                if (!typesMatch(m.getReturnType(), m.getGenericReturnType(), field)) {
                    throw new BulletDSLException("Found method's return type does not match field's type/subtype: " + field);
                }
            } else if (!Map.class.isAssignableFrom(m.getReturnType())) {
                throw new BulletDSLException("Found base method's return type is not map: " + field);
            }
            m.setAccessible(true);
            accessors.put(token[0], new Pair<>(m, null));
            return true;
        } catch (NoSuchMethodException ignored) {
            return false;
        }
    }

    private boolean setFieldAccessor(String[] token, BulletRecordField field) throws BulletDSLException {
        try {
            Field f = pojoType.getDeclaredField(token[0]);
            if (token.length == 1) {
                if (!typesMatch(f.getType(), f.getGenericType(), field)) {
                    throw new BulletDSLException("Found member's type does not match field's type/subtype: " + field);
                }
            } else if (!Map.class.isAssignableFrom(f.getType())) {
                throw new BulletDSLException("Found base member's type is not map: " + field);
            }
            f.setAccessible(true);
            accessors.put(token[0], new Pair<>(null, f));
            return true;
        } catch (NoSuchFieldException ignored) {
            return false;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public BulletRecord convert(Object object, BulletRecord record) throws BulletDSLException {
        if (!pojoType.isInstance(object)) {
            throw new BulletDSLException("Object is not of type: " + pojoType);
        }
        if (schema != null) {
            return super.convert(object, record);
        }
        // no bullet dsl schema
        accessors.forEach(
            (name, accessor) -> {
                try {
                    Serializable value = (Serializable) accessor.getValue().get(object);
                    if (value != null) {
                        record.typedSet(name, new TypedObject(value));
                    }
                } catch (IllegalAccessException ignored) {
                }
            }
        );
        return record;
    }

    @Override
    protected Object get(Object object, String base) {
        try {
            Pair<Method, Field> accessor = accessors.get(base);
            Method m = accessor.getKey();
            return m != null ? m.invoke(object) : accessor.getValue().get(object);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Helper function that checks if the specified type (and generic type) matches the type of the specified record field.
     *
     * @param type The outer type to check.
     * @param genericType The generic type to check for lists and maps.
     * @param recordField The record field to check against.
     * @return True if the types match the record field.
     */
    private static boolean typesMatch(Class type, Type genericType, BulletRecordField recordField) {
        com.yahoo.bullet.typesystem.Type recordFieldType = recordField.getType();
        try {
            // Record case
            if (recordFieldType == null) {
                if (Map.class.isAssignableFrom(type)) {
                    ParameterizedType pt = (ParameterizedType) genericType;
                    Class keyType = (Class) pt.getActualTypeArguments()[0];
                    return keyType == String.class;
                }
                return false;
            } else if (com.yahoo.bullet.typesystem.Type.isPrimitive(recordFieldType)) {
                return type == recordFieldType.getUnderlyingClass();
            } else if (com.yahoo.bullet.typesystem.Type.isPrimitiveList(recordFieldType)) {
                if (List.class.isAssignableFrom(type)) {
                    ParameterizedType pt = (ParameterizedType) genericType;
                    Type listType = pt.getActualTypeArguments()[0];
                    return listType == recordFieldType.getSubType().getUnderlyingClass();
                }
                return false;
            } else if (com.yahoo.bullet.typesystem.Type.isComplexList(recordFieldType)) {
                if (List.class.isAssignableFrom(type)) {
                    ParameterizedType pt = (ParameterizedType) genericType;
                    ParameterizedType listType = (ParameterizedType) pt.getActualTypeArguments()[0];
                    if (Map.class.isAssignableFrom((Class) listType.getRawType())) {
                        Class keyType = (Class) listType.getActualTypeArguments()[0];
                        Class valueType = (Class) listType.getActualTypeArguments()[1];
                        return keyType == String.class && valueType == recordFieldType.getSubType().getSubType().getUnderlyingClass();
                    }
                }
                return false;
            } else if (com.yahoo.bullet.typesystem.Type.isPrimitiveMap(recordFieldType)) {
                if (Map.class.isAssignableFrom(type)) {
                    ParameterizedType pt = (ParameterizedType) genericType;
                    Class keyType = (Class) pt.getActualTypeArguments()[0];
                    Type valueType = pt.getActualTypeArguments()[1];
                    return keyType == String.class && valueType == recordFieldType.getSubType().getUnderlyingClass();
                }
                return false;
            } else if (com.yahoo.bullet.typesystem.Type.isComplexMap(recordFieldType)) {
                if (Map.class.isAssignableFrom(type)) {
                    ParameterizedType pt = (ParameterizedType) genericType;
                    Class keyType = (Class) pt.getActualTypeArguments()[0];
                    ParameterizedType valueType = (ParameterizedType) pt.getActualTypeArguments()[1];
                    if (keyType == String.class && Map.class.isAssignableFrom((Class) valueType.getRawType())) {
                        Class subKeyType = (Class) valueType.getActualTypeArguments()[0];
                        Class subValueType = (Class) valueType.getActualTypeArguments()[1];
                        return subKeyType == String.class && subValueType == recordFieldType.getSubType().getSubType().getUnderlyingClass();
                    }
                }
                return false;
            }
        } catch (Exception ignored) {
        }
        return false;
    }

    /**
     * Helper function that checks if field has a valid type.
     *
     * @param field Field to be checked.
     * @return True if valid; false otherwise.
     */
    private static boolean hasValidType(Field field) {
        Class type = field.getType();
        if (PRIMITIVES.contains(type)) {
            return true;
        }
        if (Map.class.isAssignableFrom(type)) {
            ParameterizedType pt = (ParameterizedType) field.getGenericType();
            Class keyType = (Class) pt.getActualTypeArguments()[0];
            Type valueType = pt.getActualTypeArguments()[1];
            return keyType == String.class && isValidInnerType(valueType);
        }
        if (List.class.isAssignableFrom(type)) {
            ParameterizedType pt = (ParameterizedType) field.getGenericType();
            Type listType = pt.getActualTypeArguments()[0];
            return isValidInnerType(listType);
        }
        return false;
    }

    /**
     * Helper function that checks if type is a valid inner type.
     *
     * @param type Type to be checked.
     * @return True if valid; false otherwise.
     */
    private static boolean isValidInnerType(Type type) {
        if (type instanceof Class) {
            return PRIMITIVES.contains(type);
        }
        ParameterizedType pt = (ParameterizedType) type;
        if (Map.class.isAssignableFrom((Class) pt.getRawType())) {
            Class keyType = (Class) pt.getActualTypeArguments()[0];
            try {
                Class valueType = (Class) pt.getActualTypeArguments()[1];
                return keyType == String.class && PRIMITIVES.contains(valueType);
            } catch (ClassCastException ignored) {
            }
        }
        return false;
    }
}
