import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Q1 {

    public static void main(String[] args) {
        createCustomerDataset(50000);
        createTransactionDataset(5000000);
    }

    private static void createCustomerDataset(int total) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("data/customers.csv"))) {
            for (int i = 1; i <= total; i++) {

                String name = getRandomText(10, 20);
                int age = (int) (Math.random() * (70 - 10 + 1)) + 10;
                String gender = ((int) (Math.random() * 2) == 1 ? "male" : "female");
                int countryCode = (int) (Math.random() * 10) + 1;
                double salary = (Math.random() * (10000 - 100)) + 100;
                String formattedString = String.format("%d,%s,%d,%s,%d,%.2f", i, name, age, gender, countryCode, salary);

                writer.write(formattedString);
                writer.newLine(); // Write immediately after each line
            }
            System.out.println("Created customer file: " + "data/customers.csv");
        } catch (IOException e) {
            System.out.println("Failed to create customer dataset");
            e.printStackTrace();
        }
    }

    private static void createTransactionDataset(int total) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("data/transactions.csv"))) {
            for (int i = 1; i <= total; i++) {

                int customerId = (int) (Math.random() * 50000) + 1;
                double transTotal = (Math.random() * (1000 - 10)) + 10;
                int transNumItems = (int) (Math.random() * 10) + 1;
                String transDesc = getRandomText(20, 50);
                String formattedString = String.format("%d,%d,%.2f,%d,%s", i, customerId, transTotal, transNumItems, transDesc);

                writer.write(formattedString);
                writer.newLine(); // Write immediately after each line
            }
            System.out.println("Created transaction file: " + "data/transaction.csv");
        } catch (IOException e) {
            System.out.println("Failed to create transaction dataset");
            e.printStackTrace();
        }
    }

    private static String getRandomText(int startLen, int endLen) {
        StringBuilder builder = new StringBuilder();
        int len = (int) (Math.random() * (endLen-startLen+1)) + startLen;
        for (int i = 0; i < len; i++)
            builder.append((char) ('a' + (int) (Math.random() * 26)));
        return builder.toString();
    }

}
