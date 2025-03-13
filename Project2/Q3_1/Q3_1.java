import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Q3_1 {

    public static void main(String[] args) {
        createKPoints(Integer.parseInt(args[0]));
    }

    private static void createKPoints(int k) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("data/k-points.csv"))) {
            for (int i = 1; i <= k; i++) {

                int x = getRandomPoint();
                int y = getRandomPoint();

                String formattedString = String.format("%d,%d", x, y);

                writer.write(formattedString);
                writer.newLine();
            }
            System.out.println("Created k points file: " + "data/k-points.csv");
        } catch (IOException e) {
            System.out.println("Failed to create k points dataset");
            e.printStackTrace();
        }
    }

    private static int getRandomPoint() {
        return (int) (Math.random() * 10000) + 1;
    }
}
