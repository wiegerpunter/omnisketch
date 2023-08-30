package omniVLDB;

import java.io.IOException;
import java.util.ArrayList;

public class AnalysisBaselines {
    Synopsis s;
    Dataset d;
    Helper h;
    //ExpWorkload ew;


    long totalTime;
    int totalExecQueries;
    ArrayList<Double> error;

    int totalQueriesZero = 0;
    public AnalysisBaselines(Synopsis s, Dataset d, Helper h, long timepassed) throws IOException {
        this.s = s;
        this.d = d;
        this.h = h;
        //this.ew = ew;
        h.setRamSettingInfo(d, s, (double) timepassed/d.size);
    }

    public void run() throws IOException {
        error = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        //Main.logger.info("Randomly chosen queries: " + ew.randChosenQueries.length);
        //System.out.println("Randomly chosen queries: " + ew.randChosenQueries.length);
        if (Main.rangeQueries) {
            for (Query q : d.rangeQueries) {
//            if (q.idxHighestAttr >= Main.numAttributes) {
//                continue;
//            }
                computeError(q);
                h.addQueryResult(q);
            }
        } else {
            for (Query q : d.queries) {
//            if (q.idxHighestAttr >= Main.numAttributes) {
//                continue;
//            }
                computeError(q);
                h.addQueryResult(q);
            }
        }
        long endTime = System.currentTimeMillis();
        long timePassed = endTime - startTime;
        Main.logger.info("Time passed for queries: " + timePassed + " ms, average: "
                + (double) timePassed / d.queries.size() + " ms");
        System.out.println("Time passed for queries: " + timePassed + " ms, average: "
                + (double) timePassed / d.queries.size() + " ms");
    }

    public void computeError (Query q) {
        //q.exactAnswer = d.exactSolution(q);
        if (q.exactAnswer == 0) {
            totalQueriesZero++;
        }
        long startTime = System.currentTimeMillis();
        if (Main.rangeQueries) {
            q.estimate = s.rangeQuery(q);
        } else {
            q.estimate = s.query(q);
        }
        long endTime = System.currentTimeMillis();
        q.execTime = endTime - startTime;
        totalTime = totalTime + q.execTime;
        totalExecQueries++;
        boolean queryWithinBound;
        if (q.thrm33Case2) {
            queryWithinBound = Math.abs(q.estimate - q.exactAnswer) < Main.eps * d.size;
        }  else {
            queryWithinBound = q.exactAnswer <= q.estimate * 2;
        }
        q.setResult(queryWithinBound);
        q.setBound(d.size * Main.eps);
        q.setEpsError(d.dataset.size());
    }

}
