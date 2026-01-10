public class TestPartitionKeys {
    public static void main(String[] args) {
        String[] testSymbols = {"MSFT", "AAPL", "GOOGL", "AMZN", "TSLA", "NVDA", "JPM", "BAC", "WMT", "DIS"};
        int partitions = 3;
        
        System.out.println("Testing which partition each symbol would go to:");
        for (String symbol : testSymbols) {
            int hash = symbol.hashCode();
            int partition = Math.abs(hash) % partitions;
            System.out.printf("Symbol: %s, Hash: %d, Partition: %d (abs(%d) %% %d = %d)%n", 
                symbol, hash, partition, hash, partitions, partition);
        }
        
        // Find symbols for each partition
        System.out.println("\nLooking for symbols that map to each partition:");
        String[] moreSymbols = {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", 
                               "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
                               "AA", "AB", "AC", "AD", "AE", "AF", "AG", "AH", "AI", "AJ"};
        
        java.util.Map<Integer, String> partitionExamples = new java.util.HashMap<>();
        for (String symbol : moreSymbols) {
            int hash = symbol.hashCode();
            int partition = Math.abs(hash) % partitions;
            
            if (!partitionExamples.containsKey(partition)) {
                partitionExamples.put(partition, symbol);
                System.out.printf("Partition %d: Symbol '%s' (hash: %d)%n", partition, symbol, hash);
            }
            
            if (partitionExamples.size() == partitions) {
                break;
            }
        }
        
        // If we didn't find all partitions, try some common symbols
        if (partitionExamples.size() < partitions) {
            String[] commonSymbols = {"IBM", "ORCL", "CSCO", "INTC", "AMD", "QCOM", "TXN", "ADBE", "CRM", "NOW"};
            for (String symbol : commonSymbols) {
                int hash = symbol.hashCode();
                int partition = Math.abs(hash) % partitions;
                
                if (!partitionExamples.containsKey(partition)) {
                    partitionExamples.put(partition, symbol);
                    System.out.printf("Partition %d: Symbol '%s' (hash: %d)%n", partition, symbol, hash);
                }
            }
        }
    }
}
