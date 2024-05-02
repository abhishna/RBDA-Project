import java.util.HashMap;
import java.util.Map;

public class ZipCodeData {

    private static final Map<String, float[]> zipCodeMap = new HashMap<>();

    static {
        zipCodeMap.put("60601", new float[]{41.885310f, -87.622116f});
        zipCodeMap.put("60602", new float[]{41.883073f, -87.629149f});
        zipCodeMap.put("60603", new float[]{41.880188f, -87.625509f});
        zipCodeMap.put("60604", new float[]{41.878095f, -87.628461f});
        zipCodeMap.put("60605", new float[]{41.867566f, -87.617228f});
        zipCodeMap.put("60606", new float[]{41.882066f, -87.637349f});
        zipCodeMap.put("60607", new float[]{41.874930f, -87.651596f});
        zipCodeMap.put("60608", new float[]{41.846880f, -87.670664f});
        zipCodeMap.put("60609", new float[]{41.812680f, -87.656935f});
        zipCodeMap.put("60610", new float[]{41.906772f, -87.632231f});
        zipCodeMap.put("60611", new float[]{41.895700f, -87.613775f});
        zipCodeMap.put("60612", new float[]{41.880320f, -87.687749f});
        zipCodeMap.put("60613", new float[]{41.956949f, -87.654272f});
        zipCodeMap.put("60614", new float[]{41.922714f, -87.649577f});
        zipCodeMap.put("60615", new float[]{41.801647f, -87.596288f});
        zipCodeMap.put("60616", new float[]{41.844883f, -87.624032f});
        zipCodeMap.put("60617", new float[]{41.718197f, -87.552739f});
        zipCodeMap.put("60618", new float[]{41.946962f, -87.702548f});
        zipCodeMap.put("60619", new float[]{41.743690f, -87.605526f});
        zipCodeMap.put("60620", new float[]{41.740497f, -87.652558f});
        zipCodeMap.put("60621", new float[]{41.776382f, -87.639572f});
        zipCodeMap.put("60622", new float[]{41.902172f, -87.683337f});
        zipCodeMap.put("60623", new float[]{41.848897f, -87.717661f});
        zipCodeMap.put("60624", new float[]{41.880504f, -87.724444f});
        zipCodeMap.put("60625", new float[]{41.973292f, -87.700351f});
        zipCodeMap.put("60626", new float[]{42.010019f, -87.667095f});
        zipCodeMap.put("60628", new float[]{41.690875f, -87.615773f});
        zipCodeMap.put("60629", new float[]{41.775868f, -87.711496f});
        zipCodeMap.put("60630", new float[]{41.972071f, -87.756569f});
        zipCodeMap.put("60631", new float[]{41.994855f, -87.813003f});
        zipCodeMap.put("60632", new float[]{41.810166f, -87.713252f});
        zipCodeMap.put("60633", new float[]{41.663809f, -87.561470f});
        zipCodeMap.put("60634", new float[]{41.946189f, -87.806117f});
        zipCodeMap.put("60636", new float[]{41.775739f, -87.669064f});
        zipCodeMap.put("60637", new float[]{41.781621f, -87.599876f});
        zipCodeMap.put("60638", new float[]{41.781430f, -87.770520f});
        zipCodeMap.put("60639", new float[]{41.920552f, -87.756054f});
        zipCodeMap.put("60640", new float[]{41.972872f, -87.662604f});
        zipCodeMap.put("60641", new float[]{41.946606f, -87.746787f});
        zipCodeMap.put("60642", new float[]{41.902042f, -87.658544f});
        zipCodeMap.put("60643", new float[]{41.700273f, -87.663267f});
        zipCodeMap.put("60644", new float[]{41.880084f, -87.756373f});
        zipCodeMap.put("60645", new float[]{42.008558f, -87.694735f});
        zipCodeMap.put("60646", new float[]{41.993019f, -87.759627f});
        zipCodeMap.put("60647", new float[]{41.921215f, -87.701028f});
        zipCodeMap.put("60649", new float[]{41.763420f, -87.565879f});
        zipCodeMap.put("60651", new float[]{41.902093f, -87.740857f});
        zipCodeMap.put("60652", new float[]{41.747932f, -87.714795f});
        zipCodeMap.put("60653", new float[]{41.819962f, -87.605984f});
        zipCodeMap.put("60654", new float[]{41.892289f, -87.637271f});
        zipCodeMap.put("60655", new float[]{41.694776f, -87.703777f});
        zipCodeMap.put("60656", new float[]{41.974280f, -87.827132f});
        zipCodeMap.put("60657", new float[]{41.940293f, -87.646857f});
        zipCodeMap.put("60659", new float[]{41.991488f, -87.703986f});
        zipCodeMap.put("60660", new float[]{41.991110f, -87.663076f});
        zipCodeMap.put("60661", new float[]{41.883030f, -87.644101f});  
    }


    public static void main(String[] args) {
        return;
    }

    public static String findNearestZipCode(float latitude, float longitude) {
        double minDistance = Double.MAX_VALUE;
        String nearestZipCode = null;

        for (Map.Entry<String, float[]> entry : zipCodeMap.entrySet()) {
            float[] coordinates = entry.getValue();
            double distance = calculateDistance(latitude, longitude, coordinates[0], coordinates[1]);
            if (distance < minDistance) {
                minDistance = distance;
                nearestZipCode = entry.getKey();
            }
        }

        return nearestZipCode;
    }

    private static double calculateDistance(float lat1, float lon1, float lat2, float lon2) {
        final int R = 6371; // Radius of the earth in km

        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c; // convert to km

        return distance;
    }

}
