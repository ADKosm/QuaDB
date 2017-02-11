package dbms;

import dbms.command.CommandManager;
import dbms.command.CommandResult;
import dbms.query.QueryManager;
import dbms.schema.SchemaManager;

import java.io.*;
import java.net.Socket;

public class RequestHandler {
    private Socket client;

    RequestHandler(Socket client) {
        this.client = client;
    }

    int handleRequest() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));) {
            String userInput;
            String userResponse;

            CommandManager commandManager;
            SchemaManager schemaManager = new SchemaManager();
            QueryManager queryManager = new QueryManager();

            CommandResult commandResult;

            while ((userInput = in.readLine()) != null) {
                commandManager = new CommandManager(userInput);
                System.out.println("Received message from " + Thread.currentThread().getName() + " : " + userInput);
                if (commandManager.getCommandType() == Consts.CONTROL_COMMAND) {
                    commandResult = commandManager.executeCommand();
                    if (commandResult.getStatus() == Consts.STATUS_COMMAND_EXIT) {
                        writer.write(Consts.MESSAGE_BYE);
                        System.out.println("Connection closed by client");
                        break;
                    } else {
                        userResponse = Consts.MESSAGE_WARNING_UNKNOWN_COMMAND + userInput;
                        writer.write(userResponse);
                        System.out.println("Sent to client" + Thread.currentThread().getName() + ": " + userResponse);
                    }
                } else if (commandManager.getCommandType() == Consts.DDL_COMMAND) {
                    commandResult = schemaManager.executeCommand(userInput);
                    if (commandResult.getStatus() == Consts.STATUS_COMMAND_OK) {
                        writer.write(commandResult.toConsoleString());
                    } else {
                        userResponse = Consts.MESSAGE_WARNING_INVALID_COMMAND + userInput;
                        writer.write(userResponse);
                        System.out.println("Sent to client" + Thread.currentThread().getName() + ": " + userResponse);
                    }
                } else if (commandManager.getCommandType() == Consts.DML_COMMAND) {
                    commandResult = queryManager.executeCommand(userInput);
                    if (commandResult.getStatus() == Consts.STATUS_COMMAND_OK) {
                        writer.write(commandResult.toConsoleString());
                    } else {
                        userResponse = Consts.MESSAGE_WARNING_INVALID_QUERY + userInput;
                        writer.write(userResponse);
                        System.out.println("Sent to client" + Thread.currentThread().getName() + ": " + userResponse);
                    }
                } else {
                    userResponse = Consts.MESSAGE_WARNING_UNKNOWN_COMMAND + userInput;
                    writer.write(userResponse);
                    System.out.println("Sent to client" + Thread.currentThread().getName() + ": " + userResponse);
                }
                writer.newLine();
                writer.flush();
            }

            return 0;
        } catch (IOException e) {
            System.out.println("I/O exception: " + e);
            return 1;
        } catch (Exception ex) {
            System.out.println("Exceprion in Thread Run. Exception : " + ex);
            return 1;
        }
    }

}
