package employees.common;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class HelperMethods {
    public static void waitForMili(long delay) {
        try {
            TimeUnit.MILLISECONDS.sleep(delay);
        } catch (InterruptedException e1) {
            System.err.println(Arrays.toString(e1.getStackTrace()));
            waitForMili(100);
        }
    }
}
