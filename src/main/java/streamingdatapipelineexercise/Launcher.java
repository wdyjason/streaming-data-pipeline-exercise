package streamingdatapipelineexercise;

public class Launcher {
    public static void main(String[] args) throws ClassNotFoundException {
        String mainClass = args[0];
        Class<?> aClass = Class.forName(mainClass);

        System.out.println(aClass);
    }
}
