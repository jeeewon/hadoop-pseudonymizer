public class App {
    public static void main(String[] args) {
        try {
            if (args.length == 0) {
                System.err.println("Usage: App <command> [<args>]");
                System.err.println("Commands: preprocessor, pseudonymize, join");
                System.err.println("ex) App preprocessor <input> <output> <config>");
                System.err.println("ex) App pseudonymize <input> <output> <config>");
                System.err.println("ex) App join <input/tableA> <input/tableB> <output> <config>");
                System.exit(-1);
            }

            String command = args[0];
            String[] commandArgs = new String[args.length - 1];
            System.arraycopy(args, 1, commandArgs, 0, commandArgs.length);

            switch (command) {
                case "preprocessor":
                    if (args.length != 4) {
                        System.err.println("Usage: App preprocessor <input> <output> <config>");
                        System.exit(-1);
                    }
                    Pseudonymize.main(commandArgs);
                    break;
                case "pseudonymize":
                    if (args.length != 4) {
                        System.err.println("Usage: App pseudonymize <input> <output> <config>");
                        System.exit(-1);
                    }
                    Pseudonymize.main(commandArgs);
                    break;
                case "join":
                    if (args.length != 5) {
                        System.err.println("Usage: App join <input/tableA> <input/tableB> <output> <config>");
                        System.exit(-1);
                    }
                    Join.main(commandArgs);
                    break;
                default:
                    System.err.println("Unknown command: " + command);
                    System.exit(-1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}



