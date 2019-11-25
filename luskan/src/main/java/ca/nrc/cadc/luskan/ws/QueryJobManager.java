
package ca.nrc.cadc.luskan.ws;

import ca.nrc.cadc.luskan.impl.CvodbQueryRunner;
import ca.nrc.cadc.auth.ACIdentityManager;
import ca.nrc.cadc.uws.server.JobExecutor;
import ca.nrc.cadc.uws.server.JobPersistence;
import ca.nrc.cadc.uws.server.SimpleJobManager;
import ca.nrc.cadc.uws.server.ThreadPoolExecutor;
import ca.nrc.cadc.uws.server.impl.PostgresJobPersistence;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class QueryJobManager extends SimpleJobManager
{
    private static final Logger log = Logger.getLogger(QueryJobManager.class);

    private static final Long MAX_EXEC_DURATION = new Long(4*3600L);    // 4 hours to dump a catalog to vpsace
    private static final Long MAX_DESTRUCTION = new Long(7*24*60*60); // 1 week
    private static final Long MAX_QUOTE = new Long(24*3600L);         // 24 hours since we have a threadpool with queued jobs

    public QueryJobManager()
    {
        super();
        // persist UWS jobs to PostgreSQL.
        JobPersistence jobPersist = new PostgresJobPersistence(new ACIdentityManager());

        // max threads: 6 == number of simultaneously running async queries (per
        // web server), plus sync queries, plus VOSI-tables queries
        JobExecutor jobExec = new ThreadPoolExecutor(jobPersist, CvodbQueryRunner.class, 6);

        super.setJobPersistence(jobPersist);
        super.setJobExecutor(jobExec);
        super.setMaxExecDuration(MAX_EXEC_DURATION);
        super.setMaxDestruction(MAX_DESTRUCTION);
        super.setMaxQuote(MAX_QUOTE);
    }
}
