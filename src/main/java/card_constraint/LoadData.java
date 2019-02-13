package card_constraint;

import au.com.bytecode.opencsv.CSVReader;
import com.google.gson.Gson;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;
import scala.Array;
import scala.Int;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class LoadData {

    @Context
    public GraphDatabaseService db;

    @Procedure(value = "card_constraint.initialize_parse_csv")
    @Description("parse input data from file by manually parsing CSV file")
    public void initialize_parse_csv() {

        List<String[]> entries = readCSVFile("/Users/phdtest/Desktop/data.csv");

        System.out.println("Number of entries: " + entries.size());

        List<String> queries = new ArrayList<>();
/*
        for (String[] entry :
                entries) {

            uniqueDepts.putIfAbsent(entry[0], entry[1]);
            uniqueDepts.putIfAbsent(entry[2], entry[3]);
            uniquePrograms.putIfAbsent(entry[4], entry[5]);
            uniqueFunds.putIfAbsent(entry[7], entry[8]);
            uniqueAccounts.putIfAbsent(entry[9], entry[10]);
            uniqueYears.add(entry[12]);
        }

        Iterator deptIterator = uniqueDepts.entrySet().iterator();
        Iterator programIterator = uniquePrograms.entrySet().iterator();
        Iterator fundsIterator = uniqueFunds.entrySet().iterator();
        Iterator accountsIterator = uniqueAccounts.entrySet().iterator();
        Iterator yearsIterator = uniqueYears.iterator();
*/


       /* while (deptIterator.hasNext()) {
            Map.Entry dept = (Map.Entry) deptIterator.next();

            String query = "MERGE (n:Department {Dept_Code: \"" + dept.getKey() + "\", Dept_Name: \"" + dept.getValue() + "\"" +
                    "}) RETURN n";
            queries.add(query);

        }

        while(programIterator.hasNext()){
            Map.Entry program = (Map.Entry) programIterator.next();

            String query = "MERGE (n:Program {Prog_Code: \"" + program.getKey() + "\", Program_Name: \"" + program.getValue()
                    + "\"}) RETURN n";
            queries.add(query);
        }

        while(fundsIterator.hasNext()){
            Map.Entry fund = (Map.Entry) fundsIterator.next();

            String query = "MERGE (n:Source_Fund {Source_Fund_Code: \"" + fund.getKey() + "\", Source_Fund_Name: \"" +
                    fund.getValue() + "\"}) RETURN n";
            queries.add(query);
        }

        while(accountsIterator.hasNext()){
            Map.Entry account = (Map.Entry) accountsIterator.next();

            String query = "MERGE (n:Account {Account_Code: \"" + account.getKey() + "\", Account_Name: \"" +
                    account.getValue() + "\"}) RETURN n";
            queries.add(query);
        }

        while(yearsIterator.hasNext()){

            String query = "MERGE (n:Fiscal_Year {Year: \"" + yearsIterator.next() + "\"}) RETURN n";
            queries.add(query);
        }*/

        for (String[] entry
                : entries) {

            String query = "MERGE (d2:Department {Dept_Code: \"" + entry[0] + "\", Dept_Name: \"" +
                    entry[1] + "\"}) " +
                    "MERGE (p:Program {Prog_Code: \"" + entry[4] + "\", Program_Name: \"" +
                    entry[5] + "\"}) " +
                    "MERGE (d1:Department {Dept_Code: \"" + entry[2] + "\", Dept_Name: \"" + entry[3] +
                    "\"}) " +
                    "MERGE (f:Source_Fund {Source_Fund_Code: \"" + entry[7] + "\", Source_Fund_Name: " +
                    "\"" + entry[8] + "\"}) " +
                    "MERGE (a:Account {Account_Code: \"" + entry[9] + "\", Account_Name: \"" + entry[10]
                    + "\"}) " +
                    "MERGE (y:Fiscal_Year {Year: \"" + entry[12] + "\"}) " +
                    "MERGE (d1)-[:IS_SUBDEPARTMENT_OF]->(d2) " +
                    "MERGE (d2)-[:HAS_PROGRAM]->(p) " +
                    "MERGE (p)-[:RUNS_IN]->(y) " +
                    "MERGE (f)-[:SPONSORS]->(p) " +
                    "MERGE (f)-[:USES]->(a)";

            queries.add(query);


        }

        try {
            writeResults(queries, "/Users/phdtest/Desktop/cypher.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    ;

    /*@Procedure(value = "card_constraint.initialize_load_csv")
    @Description("parse input data from file by using LOAD CSV clause")
    public void initialize_load_csv(@Name("constraint_mode") String mode ) {

        List<String> queries = new ArrayList<>();

        if(mode.toLowerCase().equals(CONSTRAINT_MODE.NO_CARDINALITY)){
            String query = "LOAD CSV FROM \"file:///data.csv\" AS line " +
                    "MERGE (d1:Department {Dept_Code: line[2], Dept_Name: line[3]}) " +
                    "MERGE (d2:Department {Dept_Code: line[0], Dept_Name: line[1]}) " +
                    "MERGE (p:Program {Prog_Code: line[4], Program_Name: line[5]}) " +
                    "MERGE (f:Source_Fund {Source_Fund_Code: line[7], Source_Fund_Name: line[8]}) " +
                    "MERGE (a:Account {Account_Code: line[9], Account_Name: line[10]}) " +
                    "MERGE (y:Fiscal_Year {Year: line[12]}) " +
                    "MERGE (d1)-[:IS_SUBDEPARTMENT_OF]->(d2)-[:HAS_PROGRAM]->(p)-[:RUNS_IN]->(y) " +
                    "MERGE (f)-[:SPONSORS]->(p) " +
                    "MERGE (f)-[:USES]->(a)";

            queries.add(query);
        }

        else{
            String initialQuery = "LOAD CSV FROM \"file:///data.csv\" AS line " +
                    "MERGE (d1:Department {Dept_Code: line[2], Dept_Name: line[3]}) " +
                    "MERGE (d2:Department {Dept_Code: line[0], Dept_Name: line[1]}) " +
                    "MERGE (p:Program {Prog_Code: line[4], Program_Name: line[5]}) " +
                    "MERGE (f:Source_Fund {Source_Fund_Code: line[7], Source_Fund_Name: line[8]}) " +
                    "MERGE (a:Account {Account_Code: line[9], Account_Name: line[10]}) " +
                    "MERGE (y:Fiscal_Year {Year: line[12]})";
            queries.add(initialQuery);

        }

        try {
            writeResults(queries, "/Users/martinasestak/Desktop/create_load_csv.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }*/

    @Procedure(value = "card_constraint.load", mode = Mode.WRITE)
    @Description("create nodes and relationships")
    public void load_simple() {

        List<String> queries = readTXTFile("/Users/phdtest/Desktop/cypher.txt");


        for(int i = 0; i < queries.size(); i++){
            db.execute(queries.get(i));
        }
    }

    /*@Procedure(value = "card_constraint.load_cardinality", mode = Mode.WRITE)
    @Description("create nodes and relationships conformed with cardinality constraints")
    public void load_cardinality() {
        String filePath = "/Users/martinasestak/Desktop/data.csv";
        List<String> outputData = new ArrayList<>();
        List<String[]> entries = readCSVFile(filePath);

        List<LocalCardinalityConstraint> constraints = retrieveConstraints();

        Iterator constraintIterator = constraints.iterator();

        while(constraintIterator.hasNext()){
            LocalCardinalityConstraint c = (LocalCardinalityConstraint) constraintIterator.next();

            String constraintPattern= "(n1:" + c.nodeLabel + ")-[r1:" + c.relType + "]->";
            constraintPattern = buildSubgraphPattern(2, constraintPattern, c.subgraph);
            System.out.println("Cardinality max: " + c.maxKCard);
            Map<String, Integer> counterMap = new HashMap<>();
            switch (constraintPattern){
                case "(n1:Department)-[r1:IS_SUBDEPARTMENT_OF]->(n2:Department)":

                    for (String[] entry:
                         entries) {
                        if(counterMap.containsKey(entry[2])){
                            int currentNoRels = (int)counterMap.get(entry[2]);
                            if(currentNoRels <= c.maxKCard.intValue()){
                                counterMap.computeIfPresent(entry[2] , (k, v) -> v+1);
                                outputData.add("MERGE (n1:Department {Dept_Code: \"" + entry[2] + "\", Dept_Name: \"" + entry[3] +
                                        "\"})-[r1:IS_SUBDEPARTMENT_OF]->(n2:Department {Dept_Code: \"" + entry[0] + "\", " +
                                        "Dept_Name: \""  + entry[1] + "\"})");
                            }
                        }
                        else{
                            counterMap.put(entry[2], 1);
                        }
                    }
                    break;
                case "(n1:Source_Fund)-[r1:SPONSORS]->(n2:Program)-[r2:RUNS_IN]->(n3:Fiscal_Year)":

                    for (String[] entry:
                            entries) {
                        if(counterMap.containsKey(entry[7] + "-" + entry[12])){
                            int currentNoRels = (int)counterMap.get(entry[7] + "-" + entry[12]);
                            if(currentNoRels <= c.maxKCard.intValue()){
                                counterMap.computeIfPresent(entry[7] + "-" + entry[12] , (k, v) -> v+1);
                                outputData.add("MERGE (n1:Source_Fund {Source_Fund_Code: \"" + entry[7] + "\", " +
                                        "Source_Fund_Name: \"" + entry[8] + "\"})-[r1:SPONSORS]->(n2:Program {Prog_Code: \""
                                        + entry[4] + "\", Program_Name: \"" + entry[5] + "\"})-[r2:RUNS_IN]->(n3:Fiscal_Year " +
                                        "{Year: \"" + entry[12] + "\"})");
                            }
                        }
                        else{
                            counterMap.put(entry[7] + "-" + entry[12] , 1);
                        }
                    }
                    break;
                case "(n1:Source_Fund)-[r1:USES]->(n2:Account)":

                    for (String[] entry:
                            entries) {
                        if(counterMap.containsKey(entry[7])){
                            int currentNoRels = (int)counterMap.get(entry[7]);
                            if(currentNoRels <= c.maxKCard.intValue()){
                                counterMap.computeIfPresent(entry[7] , (k, v) -> v+1);
                                outputData.add("MERGE (n1:Source_Fund {Source_Fund_Code: \"" + entry[7] + "\", Source_Fund_Name:" +
                                        " \"" + entry[8] + "\"})-[r1:USES]->(n2:Account {Account_Code: \"" + entry[9] + "\"," +
                                        " Account_Name: \"" + entry[10] + "\"})");
                            }
                        }
                        else{
                            counterMap.put(entry[7], 1);
                        }
                    }
                    break;
            }

        }
        try {
            writeResults(outputData, "/Users/martinasestak/Desktop/import_data.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (String query :
                outputData) {
            db.execute(query);
        }
    }
*/
    @Procedure(value = "card_constraint.reset", mode = Mode.WRITE)
    @Description("delete all nodes and relationships")
    public void reset() {

        String query = "MATCH (n)-[r]->(m) DELETE n, r, m";

        db.execute(query);

    }

    @Procedure(value = "card_constraint.generate", mode = Mode.WRITE)
    @Description("generate nodes and relationships")
    public void generateGraph(@Name("no_nodes")long noNodes, @Name("index")long index) {

        String degrees = "[";

        for(int i = 0; i < noNodes; i++){
            if((i+1) == noNodes)
                degrees += "1]";
            else
                degrees += "1,";
        }
        db.execute("CALL apoc.generate.simple(" + degrees + ", 'Label" + index + "', 'Type" + index + "')");

    }

    @Procedure(value = "card_constraint.generateComplete", mode = Mode.WRITE)
    @Description("generate complete graph")
    public void generateCompleteGraph(@Name("no_nodes")long noNodes, @Name("index")long index) {

        db.execute("CALL apoc.generate.complete(" + noNodes + ", 'Label" + index + "', 'Type" + index + "')");

    }

    @Procedure(value = "card_constraint.check_constraint", mode = Mode.WRITE)
    @Description("check for relationships in database that break the cardinality constraints")
    public void checkCardinality(@Name("filePath")String path) {
        List<LocalCardinalityConstraint> constraints = retrieveConstraints();

        Iterator iterator = constraints.iterator();
        List<Relationship> listRegularRels = new ArrayList<>();
        List<String> outputResult = new ArrayList<>();

        List<String> outputDeleteCmds = new ArrayList<>();

        Map<String, Integer> dataMap = new HashMap<>();

        Set<Relationship> uniqueRedundantRels = new HashSet<>();
        while (iterator.hasNext()) {
            dataMap.clear();
            listRegularRels.clear();
            uniqueRedundantRels.clear();

            LocalCardinalityConstraint c = (LocalCardinalityConstraint) iterator.next();

            String constraintPattern= "(n1:" + c.nodeLabel + ")-[r1:" + c.relType + "]->";
            constraintPattern = buildSubgraphPattern(2, constraintPattern, c.subgraph);

            System.out.println("\n[CHECK] Constraint:  " + constraintPattern);

            /**
             * Retrieve first node, last node and first relationships labels to retrieve number of relationships
             **/
            Pattern nodesPattern = Pattern.compile("\\((.*?)\\)");
            Matcher nodesMatcher = nodesPattern.matcher(constraintPattern);
            List<String> nodesArray = new ArrayList<>();

            while(nodesMatcher.find()){
                nodesArray.add(nodesMatcher.group(1));
            }

            String lastNodeTag = nodesArray.get(nodesArray.size()-1).split(":")[0];
            String dataQuery = "";

            if(c.k.intValue() == 1){
                dataQuery = "MATCH " + constraintPattern + " RETURN n1, r1";
            }
            else{
                dataQuery = "MATCH " + constraintPattern + " RETURN n1, " + lastNodeTag + ", r1";
            }
            System.out.println("[CHECK] Data query: " + dataQuery);
            Result dataResult = db.execute(dataQuery);

            while(dataResult.hasNext()){
                Map<String, Object> dataRow = dataResult.next();
                Node node1 = (Node)dataRow.get("n1");
                Node node2 = (Node)dataRow.get(lastNodeTag);
                Relationship rel = (Relationship)dataRow.get("r1");

                if(c.k.intValue() == 1){
                    if(dataMap.containsKey(Long.toString(rel.getStartNodeId()))){
                        int currentNoRels = (int)dataMap.get(Long.toString(rel.getStartNodeId()));
                        if(currentNoRels < c.maxKCard.intValue()){
                            dataMap.computeIfPresent(Long.toString(rel.getStartNodeId()), (k, v) -> v+1);
                            listRegularRels.add(rel);
                        }
                        else {
                            System.out.println("[CHECK] Found redundant relationship! Rel ID: " + rel.getId());
                            uniqueRedundantRels.add(rel);
                        }
                    }
                    else{
                        dataMap.put(Long.toString(rel.getStartNodeId()), 1);
                    }
                }
                else{
                    if(dataMap.containsKey(Long.toString(rel.getStartNodeId()) + "-" + Long.toString(node2.getId()))){
                        int currentRels = (int) dataMap.get(Long.toString(rel.getStartNodeId()) + "-" +
                                Long.toString(node2.getId()));
                        if(currentRels < c.maxKCard.intValue()){
                            dataMap.computeIfPresent(Long.toString(rel.getStartNodeId()) + "-" +
                                    Long.toString(node2.getId()), (k, v) -> v+1);
                            listRegularRels.add(rel);

                        }
                        else{
                            uniqueRedundantRels.add(rel);
                        }
                    }
                    else{
                        dataMap.put(Long.toString(rel.getStartNodeId()) + "-" + Long.toString(node2.getId()), 1);
                    }
                }

            }

            System.out.println("[CHECK] Regular list size: " + listRegularRels.size());
            System.out.println("[CHECK] Unique redundant set size: " + uniqueRedundantRels.size());
            System.out.println("[CHECK] Relationship counter map size: " + dataMap.entrySet().size());

            String format = "%-7s %-30s%n";
            outputResult.add(String.format(format, "Constraint ID:", c._id));
            outputResult.add(String.format(format, "Description:", constraintPattern));
            outputResult.add(String.format(format, "Cardinality:", "(" + c.minKCard + ", " + c.maxKCard + ")"));
            outputResult.add(String.format(format, "No of redundant relationships:", uniqueRedundantRels.size()));

            for (Relationship r:
                 uniqueRedundantRels) {
                if(c.k.intValue() == 1)
                    db.execute("MATCH ()-[r1]-() WHERE id(r1)=" + r.getId() + " DELETE r1");
                else
                    db.execute("MATCH ()-[r1]-()--() WHERE id(r1)=" + r.getId() + " DELETE r1");

            }
            System.out.println("[CHECK] Deleted redundant relationships!");
            /*if (c.k.longValue() == 1) {

                pattern = "(n:" + c.nodeLabel + ")-[r:" + c.relType + "]->(m:" + c.subgraph.get("E") + ")";
                System.out.println("Query k=1: " + pattern);


                Result resultRel = db.execute("MATCH " + pattern + " RETURN n, COUNT(r)");


                while (resultRel.hasNext()) {

                    int counterMatch = 0;
                    Map<String, Object> rowResult = resultRel.next();
                    long numRels = (long) rowResult.get("COUNT(r)");
                    Node n = (Node) rowResult.get("n");

                    if (!(numRels >= c.minKCard.longValue() && numRels <= c.maxKCard.longValue())) {
                        if (numRels < c.minKCard.longValue()) {
                            // System.out.println("[Constraint: " + c._id + "] Relationships missing!");
                            // writeResults( "/Users/martinasestak/Desktop/check_result.txt");
                        }
                        if (numRels > c.maxKCard.longValue()) {
                            //  System.out.println("[Constraint " + c._id + "] Too many relationships!");

                            Result resultNodesRels = db.execute("MATCH " + pattern + " RETURN n, r");

                            while (resultNodesRels.hasNext()) {
                                Map<String, Object> rowRel = resultNodesRels.next();
                                Node nRel = (Node) rowRel.get("n");

                                if (n.equals(nRel)) {
                                    Relationship r = (Relationship) rowRel.get("r");
                                    counterMatch++;

                                    if (counterMatch <= c.maxKCard.intValue()) {
                                        listRegularRels.add(r);
                                    } else {
                                        redundantConstraintCounter++;
                                        listRedundantRels.add(r);
                                    }
                                }
                            }

                        }
                    }
                }

            }*/



        }
        try {
            writeResults(outputResult, path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Procedure(value = "card_constraint.create_relationship", mode = Mode.WRITE)
    @Description("create new relationship")
    public void createRel(@Name("query") String query, @Name("constraint_mode")String mode) {
        // "(:Department)-[]->() {Dept_Code: ''})-[:IS_SUBDEPARMENT_OF]->(:Department {Dept_Code: ''})"
        int numNodes = 0;
        String inputPattern = "", pathQuery = "";

        /**
         * Parse input string and get nodes and relationships
         */
        Pattern nodesPattern = Pattern.compile("\\((.*?)\\)|\\[(.*?)\\]");
        Matcher nodesMatcher = nodesPattern.matcher(query);
        List<String> matches = new ArrayList<>();

        while(nodesMatcher.find()){

            if (nodesMatcher.group(1) != null){
                matches.add(nodesMatcher.group(1));
                pathQuery += "(" + nodesMatcher.group(1).split(":")[0] + ")-";
            }

            else if(nodesMatcher.group(2) != null)
            {
                matches.add(nodesMatcher.group(2));
                pathQuery += "[" + nodesMatcher.group(2) + "]->";
            }

        }

        pathQuery = pathQuery.substring(0, pathQuery.length()-1);

        String firstNode = matches.get(0);
        String dbQueryNoCard = "MATCH ", dbQueryCard = "MATCH ";

        for(int i = 0; i<matches.size(); i++){
            numNodes++;

            if(matches.size() > i+1){
                dbQueryNoCard += "(" + matches.get(i) + "), ";
                dbQueryCard += "(" + matches.get(i) + "), ";

                inputPattern += "(" + matches.get(i) + ")-[" + matches.get(++i) + "]->";

            }
            else{
                inputPattern += "(" + matches.get(i) + ")";
                dbQueryNoCard += "(" + matches.get(i) + ")";
                dbQueryCard += "(" + matches.get(i) + ")";

            }
        }
        System.out.println("Input pattern: " + inputPattern);
       /* String subPattern = inputPattern.replaceFirst("\\((.*?)\\)-\\[(.*?)\\]->", "");
        System.out.println("\n[CREATE] Input pattern: " + dbQuery);
        System.out.println("Subpattern: " + subPattern);*/

        // no cardinality check
        if(mode.toLowerCase().equals("no_cardinality")){
            System.out.println("[CREATE NO CARDINALITY]");
           /* inputPattern = inputPattern.replaceAll("\\{.*?\\}", "");
            inputPattern = inputPattern.replaceAll("\\s", "");

            Pattern varPattern = Pattern.compile("\\((.*?)\\)|\\[(.*?)\\]");
            Matcher varMatcher = varPattern.matcher(inputPattern);

            List<String> variableMatches = new ArrayList<>();*/

           /* while(varMatcher.find()){
                if (varMatcher.group(1) != null)
                    variableMatches.add(varMatcher.group(1).split(":")[0]);
                else if(varMatcher.group(2) != null)
                    variableMatches.add(varMatcher.group(2).split(":")[1]);
            }
*/
            //pathQuery += "(" + variableMatches.get(0) + ")-[:" + variableMatches.get(1) + "]->(" + variableMatches.get(2)
            //+ ")";

            //System.out.println("Sub pattern: " + subPattern);
           // System.out.println("Variable pattern: " + pathQuery);
            dbQueryNoCard += " CREATE " + pathQuery;
                    //"MATCH (" + firstNode + ") MERGE " + subPattern + " CREATE " + pathQuery;
            System.out.println("[CREATE] DB query: " + dbQueryNoCard);
            db.execute(dbQueryNoCard);
           // System.out.println("[CREATE] Created relationship!");
        }
        else{
            /**
             * Retrieve all constraints from database
             */
            System.out.println("[CREATE CARDINALITY]");
            List<LocalCardinalityConstraint> constraints = retrieveConstraints();

            for(LocalCardinalityConstraint c: constraints){
                if(c.k.intValue() == (numNodes-1)){

                    String constraintPattern= "(n1:" + c.nodeLabel + ")-[r1:" + c.relType + "]->";
                    constraintPattern = buildSubgraphPattern(2, constraintPattern, c.subgraph);

                    /**
                     * Remove conditions to compare patterns
                     */
                    Pattern conditionsPattern = Pattern.compile("\\{(.*?)\\}");
                    Matcher conditionsMatcher = conditionsPattern.matcher(query);

                    List<String> conditionsList = new ArrayList<>();

                    while(conditionsMatcher.find()){
                        conditionsList.add(conditionsMatcher.group(1));
                    }
                    String firstNodeCondition = conditionsList.get(0);
                    String lastNodeCondition = conditionsList.get(conditionsList.size()-1);
                  //  System.out.println("Conditions: " + firstNodeCondition + " - " + lastNodeCondition);

                    List<String> keysList = new ArrayList<>();
                    List<String> valuesList = new ArrayList<>();

                    keysList.add(firstNodeCondition.split(":")[0]);
                    keysList.add(lastNodeCondition.split(":")[0]);
                    valuesList.add(firstNodeCondition.split(":|,")[1]);
                    valuesList.add(lastNodeCondition.split(":|,")[1]);

                    inputPattern = inputPattern.replaceAll("\\{.*?\\}", "");
                    inputPattern = inputPattern.replaceAll("\\s", "");

                    Pattern varPattern = Pattern.compile("\\((.*?)\\)|\\[(.*?)\\]");
                    Matcher varMatcher = varPattern.matcher(inputPattern);

                    List<String> variableMatches = new ArrayList<>();

                    while(varMatcher.find()){
                        if (varMatcher.group(1) != null)
                            variableMatches.add(varMatcher.group(1).split(":")[0]);
                        else if(varMatcher.group(2) != null)
                            variableMatches.add(varMatcher.group(2).split(":")[1]);
                    }

                    pathQuery += "(" + variableMatches.get(0) + ")-[:" + variableMatches.get(1) + "]->(" + variableMatches.get(2)
                            + ")";

                    if(inputPattern.equals(constraintPattern)){
                        /**
                         * Retrieve first node, last node and first relationships labels to retrieve number of relationships
                         **/
                        String firstNodeLabel = matches.get(0).split(":")[0];
                        String firstRelationshipLabel = matches.get(1).split(":")[0];
                        String lastNodeLabel = matches.get(matches.size()-1).split(":")[0];

                        String countQuery = "";
                        if(c.k.intValue() == 1){//condition only first node
                            countQuery = "MATCH " + inputPattern + " WHERE " + firstNodeLabel + "." + keysList.get(0) +
                                    "=" + valuesList.get(0) + " RETURN " + firstNodeLabel + ", COUNT(" +
                                    firstRelationshipLabel + ")";
                        }
                        else {
                            countQuery = "MATCH " + inputPattern + " WHERE " + firstNodeLabel + "." + keysList.get(0) +
                                    "=" + valuesList.get(0) + " AND " + lastNodeLabel + "." + keysList.get(1) + "=" +
                                    valuesList.get(1) + " RETURN " + firstNodeLabel + ", " + lastNodeLabel +
                                    ", COUNT(" + firstRelationshipLabel + ")";
                        }
                        //System.out.println("Count query: " + countQuery);
                        Result countResult = db.execute(countQuery);

                        long numRels = 0;
                        if(!countResult.hasNext()){
                            dbQueryCard += " MERGE " + pathQuery;
                            System.out.println("[CREATE] DB query: " + dbQueryCard);
                            db.execute(dbQueryCard);
                            System.out.println("[CREATE] Created relationship!");

                        }
                        while (countResult.hasNext()) {
                            numRels = (long) countResult.next().get("COUNT(" + firstRelationshipLabel + ")");

                            System.out.println("[CREATE] Numrels: " + numRels);
                            if (numRels < c.maxKCard.intValue()) {
                                dbQueryCard += "MERGE " + pathQuery;
                                System.out.println("[CREATE] DB query: " + dbQueryCard);
                                db.execute(dbQueryCard);
                                System.out.println("[CREATE] Created relationship!");
                            }
                        }

                    }
                }
            }
        }

    }

    private List<String[]> readCSVFile(String path) {

        String[] line = new String[14];

        List<String[]> entries = new ArrayList<>();
        try {
            CSVReader reader = new CSVReader(new FileReader(path), ',');
            while ((line = reader.readNext()) != null) {
                if (!line[0].equals("Dept_Code"))
                    entries.add(line);
            }
            System.out.println("Entries size: " + entries.size());
            reader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return entries;
    }

    private List<String> readTXTFile(String path) {
        String splitBy = ";";
        String line = "";

        List<String> queries = new ArrayList<String>();

        File file = new File(path);
        try {
            Scanner input = new Scanner(file);

            while (input.hasNextLine()) {
                queries.add(input.nextLine());
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return queries;
    }

    private void writeResults(List<String> queries, String outputPath) throws IOException {

        FileWriter writer = null;

        try {
            writer = new FileWriter(outputPath);

            for (String query : queries) {
                writer.write(query);
                writer.write(System.getProperty("line.separator"));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        writer.close();

    }

    private String buildSubgraphPattern(int recursionLevel, String constraintPattern, Map<String, Object> subgraphMap){
        TreeMap sortedMap = new TreeMap();
        sortedMap.putAll(subgraphMap);

        for(Object entry : sortedMap.entrySet()){
            Map.Entry property = (Map.Entry)entry;
            switch(property.getKey().toString()){
                case "E":
                    constraintPattern += "(n" + recursionLevel + ":" + property.getValue() + ")";

                    break;
                case "R":
                    constraintPattern += "-[r" + recursionLevel + ":" + property.getValue() + "]->";

                    break;
                case "S":
                    Map sMap = (Map) property.getValue();

                    constraintPattern = buildSubgraphPattern((recursionLevel+1), constraintPattern, sMap);
                    break;
                default:
                    return constraintPattern;
            }
        }
        return constraintPattern;
    }

    public List<LocalCardinalityConstraint> retrieveConstraints(){
        List<LocalCardinalityConstraint> constraints = new ArrayList<>();
        Gson gson = new Gson();
        Map<String, Object> map = new HashMap<>();

        Result resultConstraints = db.execute("MATCH (c:Card_Constraint) RETURN c");

        while (resultConstraints.hasNext()) {
            Map<String, Object> row = resultConstraints.next();
            Node n = (Node) row.get("c");
            long id = n.getId();
            String relType = n.getProperty("R").toString();
            String nodeLabel = n.getProperty("E").toString();
            map = gson.fromJson(n.getProperty("S").toString(), map.getClass());
            Number min = (long) n.getProperty("min");
            Number max = (long) n.getProperty("max");
            Number k = (long) n.getProperty("k");

            LocalCardinalityConstraint constraint = new LocalCardinalityConstraint(id, relType, nodeLabel, map, min, max, k);

            constraints.add(constraint);

        }
        return constraints;
    }

    public enum ConstraintMode {
        NO_CARDINALITY, CARDINALITY
    }
}

