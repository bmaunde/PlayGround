/*
 * Brian Maunde
 */
package ParallelizedExt;

import java.io.*;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

//Class
public class HollingBerriesParallelExt {

    private String inProdLine;
    
    private ConcurrentHashMap<Integer, Integer> markup = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Integer, Integer> sellByDurations = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, String> badSuppliers = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, String> primeSuppliers = new ConcurrentHashMap<>();
    
    private volatile StringBuffer outputStr = new StringBuffer();
    
    private final String produceFile = "C:/input/producec.csv";
    private final String priceFile = "C:/input/pricesc.csv";
    
    private final DecimalFormat df = new DecimalFormat("0.00");
    //private final GregorianCalendar gc = new GregorianCalendar();
    private final DateFormat fmt = new SimpleDateFormat("yyyy/MM/dd");
    
    
    private final char separator = ',';
    private final char replacement = '`';

    public static void main(String[] args) {

        HollingBerriesParallelExt hb = new HollingBerriesParallelExt();
        hb.readAndPopulate();
        hb.printPrices(hb.outputStr);
    }

    private void readAndPopulate() {
        
        //ExecutorService executor = Executors.newCachedThreadPool(Executors.defaultThreadFactory());
        ExecutorService executor = Executors.newFixedThreadPool(1,Executors.defaultThreadFactory());
        
        staticFills();
        
        try (BufferedReader reader = new BufferedReader(new FileReader(produceFile))) {

            reader.readLine();
            inProdLine = reader.readLine();

            while (inProdLine != null) {
                
                executor.execute(new do_Fill(inProdLine));
                inProdLine = reader.readLine();
            }

            executor.shutdown();
            
            while (! executor.isTerminated()) {
            try { 
                     executor.awaitTermination(100, TimeUnit.MILLISECONDS);
            }catch(InterruptedException e){
                e.printStackTrace();
            }

        }

        } catch (IOException e) {

            e.printStackTrace();

        }
        

    }

    private void staticFills() {

        //Markups
        markup.put(new Integer(1), new Integer(40)); //apples
        markup.put(new Integer(2), new Integer(35)); //bananas
        markup.put(new Integer(3), new Integer(55)); //berries
        markup.put(new Integer(4), new Integer(50)); //other

        //sell By dates
        sellByDurations.put(new Integer(1), new Integer(14)); //apples
        sellByDurations.put(new Integer(2), new Integer(5)); //bananas
        sellByDurations.put(new Integer(3), new Integer(7)); //other
        sellByDurations.put(new Integer(4), new Integer(7)); //other

        //bad Supliers
        badSuppliers.put("One", "32");
        badSuppliers.put("Two", "101");

        //Prime Suppliers
        primeSuppliers.put("One", "219");
        primeSuppliers.put("Two", "204");
    }

    private void printPrices(StringBuffer outputStr) {

        try (PrintWriter print = new PrintWriter(new FileWriter(priceFile))) {

            print.println(outputStr.toString());

          } catch (IOException e) {

            e.printStackTrace();

        }
    }

    class do_Fill implements Runnable {

        private final String produceLine;
        private GregorianCalendar gc = new GregorianCalendar();

        public do_Fill(String pLine) {
            this.produceLine = pLine;
        }

        @Override
        public void run() {

            Product produceProd;
            String outputSt;
            
            System.out.println("Thread :"+ Thread.currentThread().getName());
            String pLine = removeExtraSeparators(produceLine, separator, replacement);

            String[] prodAtttr = pLine.split(",");

            produceProd = doFill(prodAtttr);

            int len = produceProd.getDescription().length();

            int p = (len >= 29) ? 29 : len;
            outputSt = "";

            for (int k = 0; k < produceProd.getUnits(); k++) {

                outputSt += "R" + df.format(produceProd.getSellingPrice()) + " " + fmt.format(produceProd.getSellByDate())
                        + " " + produceProd.getDescription().substring(0, p) + "\n";

            }
            outputStr.append(outputSt);
        }

        private Product doFill(String[] prodAttributes) {

            Product produceProd;
            int supID;
            int prodCode;
            int units;
            double rate;
            double costPrice;
            String description;
            String devDate;
            Date deliveryDate = null;
            Date sellByDate;

            char separator = ',';
            char replacement = '`';

            supID = Integer.parseInt(prodAttributes[0].replace("\"", ""));
            prodCode = Integer.parseInt(prodAttributes[1].replace("\"", ""));

            description = prodAttributes[2].replace("\"", "");
            description = description.replace(replacement, separator);

            devDate = prodAttributes[3];//.replace("\"", "");
            costPrice = Double.parseDouble(prodAttributes[4].replace("\"", ""));
            units = Integer.parseInt(prodAttributes[5].replace("\"", ""));

            try {
                //System.out.println(devDate);
                deliveryDate = (Date) fmt.parse(devDate);

            } catch (ParseException | NumberFormatException ex) {
                System.out.println("Failed to parse date with "+ ex.getLocalizedMessage()+ devDate);
            }

            // what about prime rate
            Integer key = getKey(prodCode);
            rate = ((Integer) markup.get(key)).doubleValue();
            if (primeSuppliers.containsValue(Integer.toString(supID))) {
                rate += 10;
            }

            int dur = ((Integer) sellByDurations.get(key)).intValue();
            gc.setTime(deliveryDate);
            gc.add(Calendar.DAY_OF_YEAR, dur);
            sellByDate = gc.getTime();

            Double sellingPrice = costPrice * (1 + (rate / 100));
            sellingPrice /= 100;

            sellingPrice = Double.parseDouble(df.format(sellingPrice));

            if (primeSuppliers.containsValue(Integer.toString(supID))) {
                df.format(Math.ceil(sellingPrice));
            }

            if (badSuppliers.containsValue(Integer.toString(supID))) {
                Date tmp = sellByDate;
                gc.setTime(tmp);
                gc.add(Calendar.DAY_OF_YEAR, - 3);
                sellByDate = gc.getTime();

                sellingPrice = sellingPrice >= 2.00 ? sellingPrice - 2.00 : 0.00;
            }

            produceProd = new Product(supID, prodCode, description, sellByDate, sellingPrice, units);

            return produceProd;

        }

        private String removeExtraSeparators(String input, char separator, char replacement) {
            char quote = '\"';
            char[] replStr = new char[input.length()];
            boolean inQuotes = false;
            for (int k = 0; k < input.length(); k++) {
                replStr[k] = input.charAt(k);
                if (inQuotes) {
                    if (input.charAt(k) == separator) {
                        replStr[k] = replacement;
                    }
                }
                if (input.charAt(k) == quote) {
                    inQuotes = !inQuotes;
                }

            }
            return String.valueOf(replStr);
        }

        private Integer getKey(int prodCode) {

            if (prodCode > 1100 && prodCode < 1200) {
                return new Integer(1);
            } else if (prodCode >= 1200 && prodCode < 1300) {
                return new Integer(2);
            } else if (prodCode >= 1300) {
                return new Integer(3);
            } else {
                return new Integer(4);
            }
        }
    }

//Product
    class Product {

        private int supplierID;
        private int productCode;
        private String description;
        private Date sellByDate;
        private Double sellingPrice;
        private int units;

        public Product(int supplierID, int productCode, String description, Date sellByDate, Double sellingPrice, int units) {
            this.supplierID = supplierID;
            this.productCode = productCode;
            this.description = description;
            this.sellByDate = sellByDate;
            this.sellingPrice = sellingPrice;
            this.units = units;
        }

        public Double getSellingPrice() {
            return sellingPrice;
        }

        public Date getSellByDate() {
            return sellByDate;
        }

        public String getDescription() {
            return description;
        }

        public int getProductCode() {
            return productCode;
        }

        public int getSupplierID() {
            return supplierID;
        }

        public int getUnits() {
            return units;
        }
    }
}
