package hivemall.utils.lang;

public final class NumberUtils {

    private NumberUtils() {}

    public static int parseInt(String s) {
        int endIndex = s.length() - 1;
        char last = s.charAt(endIndex);
        if(Character.isLetter(last)) {
            String numstr = s.substring(0, endIndex);
            int i = Integer.parseInt(numstr);
            switch(last) {
                case 'k':
                case 'K':
                    i *= 1000;
                    break;
                case 'm':
                case 'M':
                    i *= 1000000;
                    break;
                case 'g':
                case 'G':
                    i *= 1000000000;
                    break;
                default:
                    throw new NumberFormatException("Invalid number format: " + s);
            }
            return i;
        } else {
            return Integer.parseInt(s);
        }
    }

    public static int parseInt(String s, int defaultValue) {
        if(s == null) {
            return defaultValue;
        }
        return parseInt(s);
    }

}
