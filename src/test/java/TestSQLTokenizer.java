import com.datagrig.sql.SQLTokenizer;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;
import static org.assertj.core.api.Assertions.*;

/**
 * Created by ukman on 10/31/18.
 */

public class TestSQLTokenizer {

    public static String[] QUERIES = new String[]{
            "select * from t",
            "select t.id from t group by id order by s",
            "select * from t left join f on f.id = t.key where t.id = 2*4-(4444.444+43)",
            "select * from t where a like '444%4444' ",
            "select * from t where a like \"444%4444\"",
    };

    @Test
    public void testTokenizer() throws SQLException {
        SQLTokenizer tokenizer = new SQLTokenizer();
        for(String query : QUERIES) {
            List<SQLTokenizer.Token> tokens = tokenizer.getTokens(query);
            assertThat(tokens.stream().anyMatch(t -> t.getType() == SQLTokenizer.SQLTokenType.UNKNOWN)).as("Wrong parsing '%s'", query).isEqualTo(false);
        }
    }
}
