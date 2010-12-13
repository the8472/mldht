package lbms.plugins.mldht.indexer.db;

import org.hibernate.Hibernate;
import org.hibernate.dialect.MySQL5Dialect;
import org.hibernate.dialect.MySQL5InnoDBDialect;
import org.hibernate.dialect.function.StandardSQLFunction;
import org.hibernate.type.StandardBasicTypes;
 
public class MySQLIdxHintDialect extends org.hibernate.dialect.MySQL5InnoDBDialect
{
    public MySQLIdxHintDialect()
    {
        super();
        registerFunction("useindex", new StandardSQLFunction("useindex", StandardBasicTypes.BOOLEAN));
    }
}