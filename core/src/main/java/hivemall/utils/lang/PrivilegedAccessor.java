/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package hivemall.utils.lang;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

public final class PrivilegedAccessor {

    public static Field getField(final Class<?> clazz, final String fieldName) {
        try {
            return getField(clazz, fieldName, true);
        } catch (NoSuchFieldException e) {
            throw new Error(e);
        }
    }

    public static Field getField(final Class<?> clazz, final String fieldName,
            final boolean accessible) throws NoSuchFieldException {
        final Field field;
        try {
            field = AccessController.<Field>doPrivileged(new PrivilegedExceptionAction<Field>() {
                public Field run() throws NoSuchFieldException {
                    Field f = clazz.getDeclaredField(fieldName);
                    f.setAccessible(accessible);
                    return f;
                }
            });
        } catch (PrivilegedActionException e) {
            throw new AssertionError(e.getCause());
        }
        return field;
    }

    public static void setField(final Object obj, final Class<?> clazz, final String fieldName,
            final Object value) {
        final Field field;
        try {
            field = AccessController.<Field>doPrivileged(new PrivilegedExceptionAction<Field>() {
                public Field run() throws NoSuchFieldException {
                    Field f = clazz.getDeclaredField(fieldName);
                    f.setAccessible(true);
                    return f;
                }
            });
        } catch (PrivilegedActionException e) {
            throw new AssertionError(e.getCause());
        }
        try {
            field.set(obj, value);
        } catch (IllegalArgumentException arge) {
            throw new Error(arge);
        } catch (IllegalAccessException acce) {
            throw new Error(acce);
        }
    }

    public static void setField(final Object obj, final Field field, final Object value) {
        try {
            field.set(obj, value);
        } catch (IllegalArgumentException arge) {
            throw new Error(arge);
        } catch (IllegalAccessException acce) {
            throw new Error(acce);
        }
    }

    /**
     * Return the named method with a method signature matching classTypes from the given class.
     */
    public static Method getMethod(Class<?> thisClass, String methodName, Class<?>[] classTypes)
            throws NoSuchMethodException {
        if (thisClass == null) {
            throw new NoSuchMethodException("Class is not specified for method " + methodName + ".");
        }
        try {
            return thisClass.getDeclaredMethod(methodName, classTypes);
        } catch (NoSuchMethodException e) {
            return getMethod(thisClass.getSuperclass(), methodName, classTypes);
        }
    }

    public static Method getMethod(Object instance, String methodName, Class<?>[] classTypes)
            throws NoSuchMethodException {
        Method accessMethod = getMethod(instance.getClass(), methodName, classTypes);
        accessMethod.setAccessible(true);
        return accessMethod;
    }

    /**
     * Gets the value of the named field and returns it as an object.
     *
     * @param instance the object instance
     * @param fieldName the name of the field
     * @return an object representing the value of the field
     */
    public static Object getValue(Object instance, String fieldName) throws IllegalAccessException,
            NoSuchFieldException {
        Field field = getField(instance.getClass(), fieldName, true);
        return field.get(instance);
    }

    /**
     * Calls a method on the given object instance with the given argument.
     *
     * @param instance the object instance
     * @param methodName the name of the method to invoke
     * @param arg the argument to pass to the method
     * @see PrivilegedAccessor#invokeMethod(Object,String,Object[])
     */
    public static Object invokeMethod(Object instance, String methodName, Object arg)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Object[] args = new Object[1];
        args[0] = arg;
        return invokeMethod(instance, methodName, args);
    }

    /**
     * Calls a method on the given object instance with the given arguments.
     *
     * @param instance the object instance
     * @param methodName the name of the method to invoke
     * @param args an array of objects to pass as arguments
     * @see PrivilegedAccessor#invokeMethod(Object,String,Object)
     */
    public static Object invokeMethod(Object instance, String methodName, Object... args)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Class<?>[] classTypes = null;
        if (args != null) {
            classTypes = new Class[args.length];
            for (int i = 0; i < args.length; i++) {
                if (args[i] != null)
                    classTypes[i] = args[i].getClass();
            }
        }
        return getMethod(instance, methodName, classTypes).invoke(instance, args);
    }

    /**
     * Calls a static method in the given class with the given argument.
     */
    public static Object invokeStaticMethod(String className, String methodName, Object arg)
            throws SecurityException, IllegalArgumentException, NoSuchMethodException,
            ClassNotFoundException, IllegalAccessException, InvocationTargetException {
        Object[] args = new Object[1];
        args[0] = arg;
        return invokeStaticMethod(className, methodName, args);
    }

    /**
     * Calls a static method in the given class with the given arguments.
     */
    public static Object invokeStaticMethod(String className, String methodName, Object... args)
            throws SecurityException, NoSuchMethodException, ClassNotFoundException,
            IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        Class<?> primeClass = Class.forName(className);
        Class<?>[] classTypes = null;
        if (args != null) {
            classTypes = new Class[args.length];
            for (int i = 0; i < args.length; i++) {
                if (args[i] != null)
                    classTypes[i] = args[i].getClass();
            }
        }
        Method method = primeClass.getDeclaredMethod(methodName, classTypes);
        method.setAccessible(true);

        return method.invoke(method, args);
    }

    /**
     * Calls a static method with the given class and the given arguments. use this method when the
     * specified arguments includes null object.
     */
    public static Object invokeStaticMethod(String className, String methodName,
            Class<?>[] classTypes, Object... objects) throws ClassNotFoundException,
            SecurityException, NoSuchMethodException, IllegalArgumentException,
            IllegalAccessException, InvocationTargetException {
        Class<?> primeClass = Class.forName(className);
        Method method = primeClass.getDeclaredMethod(methodName, classTypes);
        method.setAccessible(true);

        return method.invoke(method, objects);
    }
}
