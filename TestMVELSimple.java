import java.util.HashMap;
import java.util.Map;

public class TestMVELSimple {
    public static void main(String[] args) {
        // Test different expression formats
        String[] expressions = {
            // Original with indentation
            "if (MsgType == \"D\") {\n  1\n} else if (MsgType == \"8\") {\n  0\n} else {\n  2\n}",
            // Without indentation
            "if (MsgType == \"D\") {\n1\n} else if (MsgType == \"8\") {\n0\n} else {\n2\n}",
            // With semicolons
            "if (MsgType == \"D\") {\n  1;\n} else if (MsgType == \"8\") {\n  0;\n} else {\n  2;\n}",
            // Single line
            "if (MsgType == \"D\") 1 else if (MsgType == \"8\") 0 else 2",
            // With explicit return
            "if (MsgType == \"D\") {\n  return 1;\n} else if (MsgType == \"8\") {\n  return 0;\n} else {\n  return 2;\n}"
        };
        
        String[] names = {"Original", "No indent", "With semicolons", "Single line", "Explicit return"};
        
        for (int i = 0; i < expressions.length; i++) {
            System.out.println("\n=== Testing: " + names[i] + " ===");
            System.out.println("Expression: " + expressions[i].replace("\n", "\\n"));
            
            // We can't actually run MVEL without the jar, but we can see the syntax
            System.out.println("Syntax analysis: " + (expressions[i].contains("\n  1") ? "Has 2-space indent" : "No indent"));
        }
    }
}
