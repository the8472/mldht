package lbms.plugins.mldht.indexer.db;

import org.hibernate.dialect.function.StandardSQLFunction;
import org.hibernate.type.StandardBasicTypes;
 
public class HSSQLIdxHintDialect extends org.hibernate.dialect.HSQLDialect
{
    public HSSQLIdxHintDialect()
    {
        super();
        registerFunction("useindex", new StandardSQLFunction("useindex", StandardBasicTypes.BOOLEAN));
    }
}