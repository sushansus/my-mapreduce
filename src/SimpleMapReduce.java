import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class SimpleMapReduce {
    public static void main(String[] args) {
        String input = "input/AComp_Passenger_data_no_error.csv";
        SimpleMapReduce mr = new SimpleMapReduce();
        // read data from file & split into multiple partitions
        List<List<Flight>> partitions = mr.readData(input, 100);

        // deliver each partition to a mapper
        int partitionsNo = partitions.size();
        Mapper[] mappers = new Mapper[partitionsNo];
        for (int i = 0; i< partitionsNo; i++) {
            mappers[i] = new Mapper();
            mappers[i].input = partitions.get(i);
        }

        // start the mappers & wait for all to finish
        for (int i = 0; i< partitionsNo; i++) {
            mappers[i].start();
            try {
                mappers[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // collect result from mappers & reduce
        List<List<Pair>> lists = new ArrayList<>();
        for (int i = 0; i< partitionsNo; i++) {
            lists.add(mappers[i].output);
        }
        Reducer reducer = new Reducer();
        reducer.input = lists;
        reducer.start();
        try {
            reducer.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Find the max result
        List<Pair> pairs = reducer.output;
        int max = Integer.MIN_VALUE;
        for (Pair p: pairs) {
            if (max < p.numOfFlight) {
                max = p.numOfFlight;
            }
        }
        // print passenger who has the number of flight equal to max
        System.out.println("The passengers who has "+max+ " (Max) flights");
        for (Pair p: pairs) {
            if (max == p.numOfFlight) {
                System.out.println(p.passengerId);
            }
        }

        //
        System.out.println("===Test===");
        test(partitions);
    }


    public static class Mapper extends Thread {
        // List to store the input Flight objects
        public List<Flight> input;
        // List to store the output Pair objects
        List<Pair> output;
        @Override
        public void run() {
            // Sort the output of the map method
            this.output = sort(this.map(input));
        }
        // Map & combine
        public Map<String, Integer> map(List<Flight> partition) {
            Map<String, Integer> result = new HashMap<>(); // passenger id -> number of flight in 1 partition
            for (Flight f: partition) {
                Integer num = result.getOrDefault(f.passengerId, 0);
                result.put(f.passengerId, num + 1);
            }
            return result;
        }

        // Sort the output of each mapper
        public List<Pair> sort(Map<String, Integer> m) {
            List<String> keys = new ArrayList<>(m.keySet());
            Collections.sort(keys);
            List<Pair> result = new ArrayList<>();
            for (String key: keys) {
                result.add(new Pair(key, m.get(key)));
            }
            return result;
        }
    }

    public static class Reducer extends Thread {
        // A list of lists of Pair objects
        List<List<Pair>> input;
        // A list of Pair objects
        List<Pair> output;
        // A method to perform the reduce operation
        public void reduce() {
            // A map to store the mapping of passenger ID to the number of flights they have taken
            Map<String, Integer> m = new HashMap();
            // Iterate over the input list of lists of Pair objects
            for (List<Pair> list: input) {
                // Iterate over the list of Pair objects
                for (Pair p: list) {
                    // Get the current number of flights for the passenger ID or 0 if it does not exist
                    Integer fno = m.getOrDefault(p.passengerId, 0);
                    // Update the number of flights for the passenger ID
                    m.put(p.passengerId, p.numOfFlight + fno);
                }
            }
            
            // Get the list of keys (passenger IDs) from the map
            List<String> ks = new ArrayList<>(m.keySet());
            // Sort the list of keys
            Collections.sort(ks);
            // Initialize the output list of Pair objects
            output = new ArrayList<>();
            // Iterate over the sorted list of keys
            for (String k: ks) {
                // Add a new Pair object to the output list with the passenger ID and the corresponding number of flights                
                output.add(new Pair(k, m.get(k)));
            }
        }

        @Override
        public void run() {
            reduce();
        }
    }
    
// This code reads data from a file specified by the input argument
// and stores the data in a list of lists, where each inner list represents
// a flight and its details. The partitionSize argument determines the
// number of flights to be stored in each inner list.
    
    public List<List<Flight>> readData(String input, int partitionSize)  {
        // Read all lines of the file and store them in a list of strings
        List<String> lines = null;
        try {
            lines = Files.readAllLines(Path.of(input));
        } catch (Exception e) {
            // Print the stack trace of the exception if there was an error reading the file
            e.printStackTrace();
        }
        System.out.println("count=" + lines.size());
        // Create a result and temporary list to store the partitions
        List<List<Flight>> result = new ArrayList<>();
        List<Flight> tmp = new ArrayList<>();
        for (String line: lines) {
            String[] elements = line.split(",");
            Flight f = new Flight();
            // Populate the Flight object with the details from the file
            f.passengerId = elements[0];
            f.flightId = elements[1];
            f.from = elements[2];
            f.to = elements[3];
            f.departure = Integer.parseInt(elements[4]);
            f.duration = Integer.parseInt(elements[5]);
            tmp.add(f);
            // If the temporary list has reached the specified partition size, add it to the result list
            // and create a new temporary list for the next partition
            if (tmp.size() == partitionSize) {
                result.add(tmp);
                tmp = new ArrayList<>();
            }
        }
        // If there are any remaining flights in the temporary list, add them to the result list        
        if (tmp.size()>0) {
            result.add(tmp);
        }
        // Return the result list of partitions of flights
        return result;
    }

// This class represents a pair of passengerId and numOfFlight
// The passengerId is a string that stores the unique identifier of the passenger
// The numOfFlight is an integer that stores the number of flights taken by the passenger    
    public static class Pair {
        // Constructor to initialize the pair with passengerId and numOfFlight
        public Pair(String passengerId, Integer numOfFlight) {
            this.passengerId = passengerId;
            this.numOfFlight = numOfFlight;
        }

        public String passengerId;
        public Integer numOfFlight;
    }
    public static class Flight {
        // Fields to store the passenger ID, flight ID, departure location, destination, departure time, and flight duration
        public String passengerId;
        public String flightId;
        public String from;
        public String to;
        public Integer departure;
        public Integer duration;
    }

    private static void test(List<List<Flight>> partitions){
        Map<String, Integer> m = new HashMap<>();
        // Loop through all the partitions of flights
        for (List<Flight> partition: partitions) {
            // Loop through all the flights in the partition
            for (Flight flight: partition) {
                // Get the current count of flights taken by the passenger or default to 0 if the passenger is not present in the HashMap                
                int v = m.getOrDefault(flight.passengerId, 0);
                // Update the count of flights taken by the passenger
                m.put(flight.passengerId, v+1);
            }
        }
        // Find the maximum value in the HashMap which represents the maximum number of flights taken by a passenger
        Integer max = Collections.max(m.values());
        // Print the message indicating the maximum number of flights taken
        System.out.println("The passengers who has "+max+ " (Max) flights");
        // Loop through all the keys in the HashMap
        for (String k: m.keySet()) {
            // Check if the value of the current key is equal to the maximum number of flights
            if (m.get(k)==max) {
                // If yes, print the passengerId of the passenger
                System.out.println(k);
            }
        }
    }
}
