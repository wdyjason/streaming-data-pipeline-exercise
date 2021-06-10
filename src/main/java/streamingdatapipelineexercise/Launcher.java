package streamingdatapipelineexercise;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

public class Launcher {
    private String mainClass;

    public Launcher(String mainClass) {
        this.mainClass = mainClass;
    }

    public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException {
        if (args.length != 1) {
            System.err.println("mainClass is not provided.");
            System.exit(1);
        }
        new Launcher(args[0]).invoke();
    }

    private Constructor<?> findConstructor(Class<?> aClass) {
        Constructor<?>[] declaredConstructors = aClass.getDeclaredConstructors();
        return Arrays.stream(declaredConstructors).filter(x -> x.getParameters().length == 0).findFirst().get();
    }

    private Class<?> findMainClass(String mainClass) throws ClassNotFoundException {
        return Class.forName(mainClass);
    }

    private void invoke() throws ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        Class<?> aClass = findMainClass(mainClass);

        Constructor<?> constructor = findConstructor(aClass);

        Object aInstance = constructor.newInstance();

        Method executeMethod = aClass.getDeclaredMethod("execute");

        executeMethod.invoke(aInstance);
    }
}
