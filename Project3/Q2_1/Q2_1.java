import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Q2_1 {

    public static void main(String[] args) {
        createPointDataset(500000);
    }

    private static void createPointDataset(int total) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("data/points.csv"))) {
            for (int i = 1; i <= total; i++) {

                int x = getRandomCoordinate();
                int y = getRandomCoordinate();

                String formattedString = String.format("%d,%d", x, y);

                writer.write(formattedString);
                writer.newLine();
            }
            System.out.println("Created customer file: " + "data/points.csv");
        } catch (IOException e) {
            System.out.println("Failed to create point dataset");
            e.printStackTrace();
        }
    }

    private static int getRandomCoordinate() {
        return (int) (Math.random() * 10000) + 1;
    }
}
