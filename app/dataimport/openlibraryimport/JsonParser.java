package dataimport.openlibraryimport;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * Created by P. Akhmedzianov on 11.03.2016.
 */
public class JsonParser {
    public static String fromJson(final JSONObject jsonObject) {
        //if didn't find document return None
        if(jsonObject == null||!jsonObject.has("docs")){
            return "None";
        }
        else{
            String res="";
            try {
                JSONObject firstDoc = (JSONObject) jsonObject.getJSONArray("docs").get(0);
                //getting comma separated subjects
                if (firstDoc.has("subject")) {
                    res += getArrayByName(firstDoc, "subject");
                } else {
                    return "None";
                }
                return res;
            } catch (JSONException e) {
                return "None";
            }
        }
    }

    // Return comma separated array elements
    private static String getArrayByName(final JSONObject jsonObject, final String arrName) {
        String res = "";
        try {
            final JSONArray array = jsonObject.getJSONArray(arrName);
            if (array.length() > 0) {
                for (int i = 0; i < array.length(); ++i) {
                    res += array.getString(i);
                    if (i < array.length() - 1) {
                        res += ",";
                    }
                }
                return res;
            } else {
                return "None";
            }
        } catch (JSONException e) {
            return "None";
        }
    }

    public static String getIsbnFromString(final String line) throws Exception {
        String resLineIsbn = line.split("\";\"")[0].replaceAll("\"", "");
        //expecting only old ISBN10 inputs, not ISBN13
        if(resLineIsbn.length()==10) {
            return resLineIsbn;
        }
        else{
            throw new Exception("Wrong isbn:^"+resLineIsbn+"^");
        }
    }
}
