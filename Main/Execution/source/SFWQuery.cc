
#ifndef SFW_QUERY_CC
#define SFW_QUERY_CC

#include "ParserTypes.h"
	
// builds and optimizes a logical query plan for a SFW query, returning the logical query plan
// 
// note that this implementation only works for two-table queries that do not have an aggregation
// 
LogicalOpPtr SFWQuery :: buildLogicalQueryPlan (map <string, MyDB_TablePtr> &allTables, map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters) {

	this->print();
	
	int numTables = tablesToProcess.size();
	bool hasAggregate = false;
	for (auto a : valuesToSelect) {
		if (a->hasAgg()) {
			hasAggregate = true;
			break;
		}
	}
	hasAggregate = hasAggregate || groupingClauses.size() > 0;

	// vector <ExprTreePtr> valuesToSelect;
	// vector <pair <string, string>> tablesToProcess;
	// vector <ExprTreePtr> allDisjunctions;
	// vector <ExprTreePtr> groupingClauses;
	// vector <string> tmpTables;

	// LogicalAggregate (LogicalOpPtr inputOp, MyDB_TablePtr outputSpec, vector <ExprTreePtr> &exprsToCompute, 
	// 	vector <ExprTreePtr> &groupings)

	// LogicalJoin (LogicalOpPtr leftInputOp, LogicalOpPtr rightInputOp, MyDB_TablePtr outputSpec,
	// 	vector <ExprTreePtr> &outputSelectionPredicate, vector <ExprTreePtr> &exprsToCompute, string leftTableAlias, string rightTableAlias)

	// LogicalTableScan (MyDB_TableReaderWriterPtr inputSpec, MyDB_TablePtr outputSpec, MyDB_StatsPtr inputStats, 
	// 	vector <ExprTreePtr> &selectionPred, vector <string> &exprsToCompute, string tableAlias)

	if (numTables == 1) {
		// single table selection with aggregates
		if (hasAggregate) {
			MyDB_TablePtr inTable = allTables[tablesToProcess[0].first];
			MyDB_SchemaPtr outSelectionSchema = make_shared <MyDB_Schema> ();
			vector<string> exprs;
			for (auto attrs: inTable->getSchema()->getAtts()) {
				bool needIt = false;
				for (auto v : valuesToSelect) {
					if (v->referencesAtt(tablesToProcess[0].second, attrs.first)) {
						needIt = true;
					}
				}
				if (needIt) {
					outSelectionSchema->getAtts().push_back(make_pair(attrs.first, attrs.second));
					exprs.push_back("["+ attrs.first +"]");
				}
			}
			tmpTables.push_back("outputSelectionStorageLoc");
			MyDB_TablePtr tableSelectionOut = make_shared <MyDB_Table> ("outputSelectionTable", "outputSelectionStorageLoc", outSelectionSchema);
			LogicalOpPtr tableScan = make_shared <LogicalTableScan> (allTableReaderWriters[tablesToProcess[0].first], 
				tableSelectionOut, make_shared <MyDB_Stats> (inTable, tablesToProcess[0].second), allDisjunctions, exprs, tablesToProcess[0].second);

			vector<ExprTreePtr> aggExprsToCompute;
			vector<ExprTreePtr> aggGroupings;

			vector<string> finalExprToCompute;

			MyDB_SchemaPtr aggSchema = make_shared <MyDB_Schema> ();
			MyDB_SchemaPtr finalSchema = make_shared <MyDB_Schema> ();
			int aggCnt = 0;
			string aggStr, exprStr;
			ExprTreePtr aggTree;
			for (auto v: valuesToSelect) {
				exprStr = v->toString();
				if (v->hasAgg()) {
					aggCnt++;
					aggTree = findAggClause(v);
					aggStr = aggTree->toString();
					aggExprsToCompute.push_back(aggTree);
					exprStr.replace(exprStr.find(aggStr), aggStr.length(), "agg_"+to_string(aggCnt));
					finalExprToCompute.push_back(exprStr);
					aggSchema->getAtts().push_back(make_pair("agg_"+to_string(aggCnt), this->getExprType(aggTree, allTableReaderWriters[tablesToProcess[0].first])));
				}
				else {
					for (auto attrs : tableSelectionOut->getSchema()->getAtts()) {
						if (v->referencesAtt(tablesToProcess[0].second, attrs.first)) {
							finalExprToCompute.push_back(exprStr);
							aggSchema->getAtts().push_back(make_pair(attrs.first, attrs.second));
							break;
						}
					}
				}
			}
			tmpTables.push_back("aggStorageLoc");
			MyDB_TablePtr tableAggOut = make_shared<MyDB_Table>("aggTable", "aggStorageLoc", aggSchema);
			LogicalOpPtr aggregate = make_shared <LogicalAggregate> (tableScan, tableAggOut, aggExprsToCompute, groupingClauses, tablesToProcess[0].second);
			// // MyDB_TableReaderWriterPtr aggOutTable = aggregate->execute();

			// // tmpTables.push_back("finalStorageLoc");
			// // MyDB_TablePtr finalTableOut = make_shared<MyDB_Table>("finalTable", "finalStorageLoc", finalSchema);			
			// // LogicalOpPtr finalOp = make_shared <LogicalTableScan> (aggOutTable, 
			// // 	finalTableOut, make_shared <MyDB_Stats> (table, tablesToProcess[0].second), allDisjunctions, exprs, tablesToProcess[0].second);

			return aggregate;
		}
		// single table selection without aggregates
		else {
			MyDB_TablePtr table = allTables[tablesToProcess[0].first];
			MyDB_SchemaPtr schema = make_shared <MyDB_Schema> ();
			vector<string> exprs;
			for (auto attrs: table->getSchema()->getAtts()) {
				bool needIt = false;
				for (auto v: valuesToSelect) {
					if (v->referencesAtt(tablesToProcess[0].second, attrs.first)) {
						needIt = true;
					}
				}
				if (needIt) {
					schema->getAtts().push_back(make_pair(attrs.first, attrs.second));
					exprs.push_back("["+ attrs.first +"]");
				}
			}

			tmpTables.push_back("outputStorageLoc");
			LogicalOpPtr tableScan = make_shared <LogicalTableScan> (allTableReaderWriters[tablesToProcess[0].first], 
				make_shared <MyDB_Table> ("outputTable", "outputStorageLoc", schema), 
				make_shared <MyDB_Stats> (table, tablesToProcess[0].second), allDisjunctions, exprs, tablesToProcess[0].second);

			return tableScan;

		}
	}
	else if (numTables == 2) {
		// two table join with aggregates
		if (hasAggregate) {

		}
		// two table join without aggregates
		else {
			// find the two input tables
			MyDB_TablePtr leftTable = allTables[tablesToProcess[0].first];
			MyDB_TablePtr rightTable = allTables[tablesToProcess[1].first];
			
			// find the various parts of the CNF
			vector <ExprTreePtr> leftCNF; 
			vector <ExprTreePtr> rightCNF; 
			vector <ExprTreePtr> topCNF; 

			// loop through all of the disjunctions and break them apart
			for (auto a: allDisjunctions) {
				bool inLeft = a->referencesTable (tablesToProcess[0].second);
				bool inRight = a->referencesTable (tablesToProcess[1].second);
				if (inLeft && inRight) {
					cout << "top " << a->toString () << "\n";
					topCNF.push_back (a);
				} else if (inLeft) {
					cout << "left: " << a->toString () << "\n";
					leftCNF.push_back (a);
				} else {
					cout << "right: " << a->toString () << "\n";
					rightCNF.push_back (a);
				}
			}

			// now get the left and right schemas for the two selections
			MyDB_SchemaPtr leftSchema = make_shared <MyDB_Schema> ();
			MyDB_SchemaPtr rightSchema = make_shared <MyDB_Schema> ();
			MyDB_SchemaPtr totSchema = make_shared <MyDB_Schema> ();
			vector <string> leftExprs;
			vector <string> rightExprs;
				
			// and see what we need from the left, and from the right
			for (auto b: leftTable->getSchema ()->getAtts ()) {
				bool needIt = false;
				for (auto a: valuesToSelect) {
					if (a->referencesAtt (tablesToProcess[0].second, b.first)) {
						needIt = true;
					}
				}
				for (auto a: topCNF) {
					if (a->referencesAtt (tablesToProcess[0].second, b.first)) {
						needIt = true;
					}
				}
				if (needIt) {
					leftSchema->getAtts ().push_back (make_pair (tablesToProcess[0].second + "_" + b.first, b.second));
					totSchema->getAtts ().push_back (make_pair (tablesToProcess[0].second + "_" + b.first, b.second));
					leftExprs.push_back ("[" + b.first + "]");
					cout << "left expr: " << ("[" + b.first + "]") << "\n";
				}
			}

			cout << "left schema: " << leftSchema << "\n";

			// and see what we need from the right, and from the right
			for (auto b: rightTable->getSchema ()->getAtts ()) {
				bool needIt = false;
				for (auto a: valuesToSelect) {
					if (a->referencesAtt (tablesToProcess[1].second, b.first)) {
						needIt = true;
					}
				}
				for (auto a: topCNF) {
					if (a->referencesAtt (tablesToProcess[1].second, b.first)) {
						needIt = true;
					}
				}
				if (needIt) {
					rightSchema->getAtts ().push_back (make_pair (tablesToProcess[1].second + "_" + b.first, b.second));
					totSchema->getAtts ().push_back (make_pair (tablesToProcess[1].second + "_" + b.first, b.second));
					rightExprs.push_back ("[" + b.first + "]");
					cout << "right expr: " << ("[" + b.first + "]") << "\n";
				}
			}
			cout << "right schema: " << rightSchema << "\n";
				
			// now we gotta figure out the top schema... get a record for the top
			MyDB_Record myRec (totSchema);
			
			// and get all of the attributes for the output
			MyDB_SchemaPtr topSchema = make_shared <MyDB_Schema> ();
			int i = 0;
			for (auto a: valuesToSelect) {
				topSchema->getAtts ().push_back (make_pair ("att_" + to_string (i++), myRec.getType (a->toString ())));
			}
			cout << "top schema: " << topSchema << "\n";
			
			// and it's time to build the query plan
			tmpTables.push_back("leftStorageLoc");
			tmpTables.push_back("rightStorageLoc");
			/*
			LogicalTableScan (MyDB_TableReaderWriterPtr inputSpec, MyDB_TablePtr outputSpec, MyDB_StatsPtr inputStats, 
				vector <ExprTreePtr> &selectionPred, vector <string> &exprsToCompute, string tableAlias)
			*/
			LogicalOpPtr leftTableScan = make_shared <LogicalTableScan> (allTableReaderWriters[tablesToProcess[0].first], 
				make_shared <MyDB_Table> ("leftTable", "leftStorageLoc", leftSchema), 
				make_shared <MyDB_Stats> (leftTable, tablesToProcess[0].second), leftCNF, leftExprs, tablesToProcess[0].second);
			LogicalOpPtr rightTableScan = make_shared <LogicalTableScan> (allTableReaderWriters[tablesToProcess[1].first], 
				make_shared <MyDB_Table> ("rightTable", "rightStorageLoc", rightSchema), 
				make_shared <MyDB_Stats> (rightTable, tablesToProcess[1].second), rightCNF, rightExprs, tablesToProcess[1].second);
			
			tmpTables.push_back("topStorageLoc");
			/*
			LogicalJoin (LogicalOpPtr leftInputOp, LogicalOpPtr rightInputOp, MyDB_TablePtr outputSpec,
				vector <ExprTreePtr> &outputSelectionPredicate, vector <ExprTreePtr> &exprsToCompute, string leftTableAlias, string rightTableAlias)
			*/
			LogicalOpPtr returnVal = make_shared <LogicalJoin> (leftTableScan, rightTableScan, 
				make_shared <MyDB_Table> ("topTable", "topStorageLoc", topSchema), topCNF, valuesToSelect, tablesToProcess[0].second, tablesToProcess[1].second);

			// done!!
			return returnVal;
		}
	}
	else {
		if (hasAggregate) {

		}
		else {

		}
	}

	return nullptr;
	
}

MyDB_AttTypePtr SFWQuery :: getExprType (ExprTreePtr tree, MyDB_TableReaderWriterPtr table) {
	// virtual bool isId () {return false;}
	// virtual bool isSum () {return false;}
	// virtual bool isAvg () {return false;}
	if (tree->isComp()) { // <, > , =
		return make_shared<MyDB_BoolAttType>();
	}
	else if (tree->isId()) { // id
		string tableAttName = tree->toString();
		string attName = tableAttName.substr(tableAttName.find('_')+1, tableAttName.length());
		cout << "check get expr type: identifier attr name: " << attName << endl;
		return table->getTable()->getSchema()->getAttByName(attName).second;
	}
	else if (tree->isSum() || tree->isAvg()) { // agg
		return getExprType(tree->getChild(), table);
	}
	else if (tree->getLHS() == nullptr && tree->getRHS() == nullptr) { // literal
		string literal = tree->toString();
		if (literal.find("bool[") == 0) {
			return make_shared<MyDB_BoolAttType>();
		}
		else if (literal.find("double[") == 0) {
			return make_shared<MyDB_DoubleAttType>();
		}
		else if (literal.find("int[") == 0) {
			return make_shared<MyDB_IntAttType>();
		}
		else {
			return make_shared<MyDB_StringAttType>();
		}
	} 
	else { // +, -, *, /
		MyDB_AttTypePtr leftAtt = this->getExprType(tree->getLHS(), table);
		MyDB_AttTypePtr rightAtt = this->getExprType(tree->getRHS(), table);
		if (leftAtt == rightAtt) {
			return leftAtt;
		}
		else {
			if (leftAtt == make_shared<MyDB_DoubleAttType>() || rightAtt == make_shared<MyDB_DoubleAttType>()) {
				return make_shared<MyDB_DoubleAttType>();
			}
			else { // string + else
				return make_shared<MyDB_StringAttType>();
			}
		}
	}
}

ExprTreePtr SFWQuery :: findAggClause (ExprTreePtr tree) {
	vector<ExprTreePtr> subTrees;
	subTrees.push_back(tree);
	ExprTreePtr currTree, leftTree, rightTree, childTree;
	while (subTrees.size() > 0) {
		currTree = subTrees[0];
		subTrees.erase(subTrees.begin());
		if (currTree->isSum() || currTree->isAvg()) {
			return currTree;
		}
		leftTree = currTree->getLHS();
		rightTree = currTree->getRHS();
		childTree = currTree->getChild();
		if (leftTree != nullptr) {
			subTrees.push_back(leftTree);
		}
		if (rightTree != nullptr) {
			subTrees.push_back(rightTree);
		}
		if (childTree != nullptr) {
			subTrees.push_back(childTree);
		}
	}
	return nullptr;
}

void SFWQuery :: print () {
	cout << "Selecting the following:\n";
	for (auto a : valuesToSelect) {
		cout << "\t" << a->toString () << "\n";
	}
	cout << "From the following:\n";
	for (auto a : tablesToProcess) {
		cout << "\t" << a.first << " AS " << a.second << "\n";
	}
	cout << "Where the following are true:\n";
	for (auto a : allDisjunctions) {
		cout << "\t" << a->toString () << "\n";
	}
	cout << "Group using:\n";
	for (auto a : groupingClauses) {
		cout << "\t" << a->toString () << "\n";
	}
}


SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause,
        struct CNF *cnf, struct ValueList *grouping) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
        allDisjunctions = cnf->disjunctions;
        groupingClauses = grouping->valuesToCompute;
}

SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause,
        struct CNF *cnf) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
		allDisjunctions = cnf->disjunctions;
}

SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
        allDisjunctions.push_back (make_shared <BoolLiteral> (true));
}

void SFWQuery :: removeTempTables () {
	for (auto &table : tmpTables) {
		remove(table.c_str());
	}
}

#endif
