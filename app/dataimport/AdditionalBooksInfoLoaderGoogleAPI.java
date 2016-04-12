package dataimport;


import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.books.Books;
import com.google.api.services.books.BooksRequestInitializer;
import com.google.api.services.books.model.Volume;
import com.google.api.services.books.model.Volumes;

import java.io.*;
import java.security.GeneralSecurityException;


/**
 * Created by P. Akhmedzianov on 10.03.2016.
 */
public class AdditionalBooksInfoLoaderGoogleAPI {
    private static final String INPUT_FILE_PATH = "input/BX-Books.csv";

    private static final String APPLICATION_NAME = "UkmaRecSys";
    private static final String API_KEY = "AIzaSyBWMxWajh-aeq5qgMdvFziPX-oGDxt5L_k";

    private static final String RESFILE_STRING_DELIMITER = "^";
    private static final String CSVFILE_STRING_DELIMITER = ";";

    private Books books;
private int counter = 0;

    public AdditionalBooksInfoLoaderGoogleAPI(){
        // Set up Books client.
        JsonFactory jsonFactory = new JacksonFactory();
        try {
            books = new Books.Builder(GoogleNetHttpTransport.newTrustedTransport(), jsonFactory, null)
                    .setApplicationName(APPLICATION_NAME)
                    .setGoogleClientRequestInitializer(new BooksRequestInitializer(API_KEY))
                    .build();

        } catch (GeneralSecurityException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }



    public void test() {
        String isbn1 = "3596292646";
        String isbn2 = "0679425608";
        try {
            queryGoogleBooks("ISBN:"+isbn1+"+OR+ISBN:"+isbn2);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void transform(){
        try (BufferedReader br = new BufferedReader(new FileReader(INPUT_FILE_PATH))) {
            File fout = new File("input/books.txt");
            FileOutputStream fos = new FileOutputStream(fout);

            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
            String line;
                   while ((line = br.readLine()) != null) {
                    String[] arr = line.split("\";\"");
                    String isbn = arr[0].replaceAll("\"", "");
                    String additionalInfo = getInfoByIsbn(isbn);
                    if (additionalInfo != "None") {
                        String resLine = isbn + RESFILE_STRING_DELIMITER + additionalInfo;
                        for (int i = 3; i < arr.length; i++) {
                            resLine += arr[i].replaceAll("\"", "");
                            if (i < arr.length - 1) {
                                resLine += RESFILE_STRING_DELIMITER;
                            }
                        }
                        counter++;
                        bw.write(resLine);
                        bw.newLine();
                    }
                }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Counter:"+counter);
    }

    public String getInfoByIsbn(String isbn)
    {
        try {
            String res =  queryGoogleBooks("isbn:"+isbn);
            // Success!
            return res;
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "None";
    }
    private String queryGoogleBooks(String query) throws Exception {
        StringBuilder resOfQuerySB = new StringBuilder();
        // Set query string
        Books.Volumes.List volumesList = books.volumes().list(query);

        // Execute the query.
        Volumes volumes = volumesList.execute();
        if (volumes.getTotalItems() == 0 || volumes.getItems() == null) {
            System.out.println("No matches found.");
            return "None";
        }
        if (volumes.getTotalItems() > 1) {
            System.out.println("Total items:"+volumes.getTotalItems());
            return "None";
        }

        // Output results.
        Volume volume = volumes.getItems().get(0);
        Volume.VolumeInfo volumeInfo = volume.getVolumeInfo();

        // Title.
/*        resOfQuerySB.append(volumeInfo.getTitle()+RESFILE_STRING_DELIMITER);*/
        // Author(s).
 /*       java.util.List<String> authors = volumeInfo.getAuthors();
        if (authors != null && !authors.isEmpty()) {
            for (int i = 0; i < authors.size(); ++i) {
                resOfQuerySB.append(authors.get(i));
                if (i < authors.size() - 1) {
                    resOfQuerySB.append(",");
                }
            }
            resOfQuerySB.append(RESFILE_STRING_DELIMITER);
        }
        else {
            resOfQuerySB.append("None"+RESFILE_STRING_DELIMITER);
        }*/
        // Description (if any).
        if (volumeInfo.getDescription() != null && volumeInfo.getDescription().length() > 0) {
            resOfQuerySB.append(volumeInfo.getDescription()/*+RESFILE_STRING_DELIMITER*/);
        }
        else {
            resOfQuerySB.append("None");
        }
        // categories(s).
/*        java.util.List<String> categories = volumeInfo.getCategories();
        if (categories != null && !categories.isEmpty()) {
            for (int i = 0; i < categories.size(); ++i) {
                resOfQuerySB.append(categories.get(i));
                if (i < categories.size() - 1) {
                    resOfQuerySB.append(",");
                }
            }
            resOfQuerySB.append(RESFILE_STRING_DELIMITER);
        }
        else {
            resOfQuerySB.append("None"+RESFILE_STRING_DELIMITER);
        }*/
        // number of pages (if not set equals 0)
/*        Integer numPages = volumeInfo.getPageCount();
        if(numPages != null) {
            resOfQuerySB.append(numPages + RESFILE_STRING_DELIMITER);
        }
        else {
            resOfQuerySB.append("None"+RESFILE_STRING_DELIMITER);
        }*/
        // language
/*        resOfQuerySB.append(volumeInfo.getLanguage()+RESFILE_STRING_DELIMITER);*/

       return (resOfQuerySB.toString());
    }
}
