package dataimport.openlibraryimport;

import org.codehaus.jettison.json.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by P. Akhmedzianov on 11.03.2016.
 */
public class OpenLibraryBooksTransformer {
    private static final String RESFILE_STRING_DELIMITER = "^";
    private static final String INPUT_FILE_PATH = "input/BX-Books1.csv";
    private static final String OUTPUT_FILE_PATH = "input/books2.txt";
    private static final int LIST_SIZE = 1000;

    //http client for getting additional book info
    private BookClient bookClient_;

    public OpenLibraryBooksTransformer() {
        bookClient_ = new BookClient();
    }


    public void transform() {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(INPUT_FILE_PATH))) {
            PrintWriter printWriter = new PrintWriter(new BufferedWriter(new FileWriter((OUTPUT_FILE_PATH))));
            ArrayList<String> tmpStringList = new ArrayList<>();
            String line;
            //reading file line by line and putting strings to ArrayList
            while ((line = bufferedReader.readLine()) != null) {
                tmpStringList.add(line);
                if (tmpStringList.size() == LIST_SIZE) {
                    this.processTheList(bookClient_.getBooksByStringMap(tmpStringList), printWriter);
                    tmpStringList = new ArrayList<String>();
                }
            }
            //processing the rest of file, which size is smaller then LIST_SIZE
            this.processTheList(bookClient_.getBooksByStringMap(tmpStringList), printWriter);
            bufferedReader.close();
            printWriter.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void processTheList(final HashMap<String, JSONObject> stringJsonObjectHashMap, final PrintWriter printWriter) {
        stringJsonObjectHashMap.forEach((key, value) -> {
            String additionalInfo = JsonParser.fromJson(value);
            //if there is no result just missing this input string
            if (additionalInfo != "None") {
                String resLine = key.replaceAll("\";\"", RESFILE_STRING_DELIMITER).replaceAll("\"", "")
                        + RESFILE_STRING_DELIMITER + additionalInfo;
                printWriter.println(resLine);
            }
        });
    }
}
