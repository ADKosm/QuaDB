package dbms;

public class Main {

    public static void main(String[] args) {
        int portNumber = Consts.PORT;

        if (args.length == 1) {
            portNumber = Integer.parseInt(args[0]);
        }

        Server server = new Server(portNumber);
        server.start();
    }
}