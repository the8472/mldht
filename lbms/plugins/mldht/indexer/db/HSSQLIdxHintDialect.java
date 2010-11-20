package lbms.plugins.mldht.indexer.db;

import org.hibernate.Hibernate;
import org.hibernate.dialect.MySQL5Dialect;
import org.hibernate.dialect.MySQL5InnoDBDialect;
import org.hibernate.dialect.function.StandardSQLFunction;
 
public class HSSQLIdxHintDialect extends org.hibernate.dialect.HSQLDialect
{
    public HSSQLIdxHintDialect()
    {
        super();
        registerFunction("useindex", new StandardSQLFunction("useindex", Hibernate.BOOLEAN));
    }
}