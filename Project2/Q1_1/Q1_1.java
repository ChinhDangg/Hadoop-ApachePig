import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Q1_1 {

    public static void main(String[] args) {
        createPointDataset(10000000);
        createRectangleDataset(4500000);
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

    private static void createRectangleDataset(int total) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("data/rectangles.csv"))) {
            for (int i = 1; i <= total; i++) {

                int x = getRandomCoordinate();
                int y = getRandomCoordinate();
                int width = (int) (Math.random() * 7) + 1;
                int height = (int) (Math.random() * 20) + 1;

                String formattedString = String.format("%s,%d,%d,%d,%d", ("r" + i), x, y, width, height);

                writer.write(formattedString);
                writer.newLine();
            }
            System.out.println("Created customer file: " + "data/rectangles.csv");
        } catch (IOException e) {
            System.out.println("Failed to create rectangle dataset");
            e.printStackTrace();
        }
    }
}
