import org.mvel2.MVEL;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TestMVEL {
    public static void main(String[] args) {
        String expression = "if (MsgType == \"D\") {\n  1\n} else if (MsgType == \"8\") {\n  0\n} else {\n  2\n}";
        
        Map<String, Object> context = new HashMap<>();
        context.put("MsgType", "D");
        context.put("totalPartitions", 3);
        
        try {
            Serializable compiled = MVEL.compileExpression(expression);
            Object result = MVEL.executeExpression(compiled, context);
            System.out.println("Result: " + result);
            System.out.println("Result type: " + (result != null ? result.getClass().getName() : "null"));
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
