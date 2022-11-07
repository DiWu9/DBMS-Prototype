
#ifndef LOG_OP_CC
#define LOG_OP_CC

#include "Aggregate.h"
#include "MyDB_LogicalOps.h"

// fill this out!  This should actually run the aggregation via an appropriate RelOp, and then it is going to
// have to unscramble the output attributes and compute exprsToCompute using an execution of the RegularSelection 
// operation (why?  Note that the aggregate always outputs all of the grouping atts followed by the agg atts.
// After, a selection is required to compute the final set of aggregate expressions)
//
// Note that after the left and right hand sides have been executed, the temporary tables associated with the two 
// sides should be deleted (via a kill to killFile () on the buffer manager)
MyDB_TableReaderWriterPtr LogicalAggregate :: execute () {
	// Aggregate (MyDB_TableReaderWriterPtr input, MyDB_TableReaderWriterPtr output,
	// 	vector <pair <MyDB_AggType, string>> aggsToCompute,
	// 	vector <string> groupings, string selectionPredicate);

	// LogicalOpPtr inputOp;
	// MyDB_TablePtr outputSpec;
	// vector <ExprTreePtr> exprsToCompute;
	// vector <ExprTreePtr> groupings;

	//return nullptr;
	MyDB_TableReaderWriterPtr filteredTable = inputOp->execute();
	MyDB_TableReaderWriterPtr outTable = make_shared<MyDB_TableReaderWriter>(outputSpec, filteredTable->getBufferMgr());
	vector<pair<MyDB_AggType, string>> aggsToCompute;

	cout << "Print aggregate attributes: " << endl;
	for (auto e : exprsToCompute) {
		cout << e->toString() << endl;
		string agg = e->getChild()->toString();
		vector<pair<string, MyDB_AttTypePtr>> tableAttrs = filteredTable->getTable()->getSchema()->getAtts();
		for (auto attr : tableAttrs) {
			string attrName = this->tableAlias + "_" + attr.first;
			size_t index = agg.find(attrName);
			while (index != string::npos) {
				agg.replace(index, attrName.length(), attr.first);
				index = agg.find(attrName);
			}
		}
		if (e->isAvg()) {
			aggsToCompute.push_back(make_pair(MyDB_AggType::avg, agg));
		}
		else if (e->isSum()) {
			aggsToCompute.push_back(make_pair(MyDB_AggType::sum, agg));
		}
		else {
			aggsToCompute.push_back(make_pair(MyDB_AggType::cnt, agg));
		}
	}

	cout << "Print grouping attributes: " << endl;
	vector<string> groupingAttrs;
	for (auto g : groupings) {
		cout << g->toString() << endl;
		groupingAttrs.push_back(g->toString());
	}
	
	Aggregate op(filteredTable, outTable, aggsToCompute, groupingAttrs, "bool[true]");
	op.run();
	filteredTable->getBufferMgr()->killTable(filteredTable->getTable());
	return outTable;

}
// we don't really count the cost of the aggregate, so cost its subplan and return that
pair <double, MyDB_StatsPtr> LogicalAggregate :: cost () {
	return inputOp->cost ();
}
	
// this costs the entire query plan with the join at the top, returning the compute set of statistics for
// the output.  Note that it recursively costs the left and then the right, before using the statistics from
// the left and the right to cost the join itself
pair <double, MyDB_StatsPtr> LogicalJoin :: cost () {
	auto left = leftInputOp->cost ();
	auto right = rightInputOp->cost ();
	MyDB_StatsPtr outputStats = left.second->costJoin (outputSelectionPredicate, right.second);
	return make_pair (left.first + right.first + outputStats->getTupleCount (), outputStats);
}
	
// Fill this out!  This should recursively execute the left hand side, and then the right hand side, and then
// it should heuristically choose whether to do a scan join or a sort-merge join (if it chooses a scan join, it
// should use a heuristic to choose which input is to be hashed and which is to be scanned), and execute the join.
// Note that after the left and right hand sides have been executed, the temporary tables associated with the two 
// sides should be deleted (via a kill to killFile () on the buffer manager)
MyDB_TableReaderWriterPtr LogicalJoin :: execute () {
	cout << "start of LogicalJoin.execute()" << endl;
	int cnt = 0;
	int numRecordsToPrint = 30;
	MyDB_RecordPtr rec;
	MyDB_RecordIteratorAltPtr iter;
	/*
	LogicalOpPtr leftInputOp;
	LogicalOpPtr rightInputOp;
	MyDB_TablePtr outputSpec;
	vector <ExprTreePtr> outputSelectionPredicate;
	vector <ExprTreePtr> exprsToCompute;
	*/
	MyDB_TableReaderWriterPtr leftTable = leftInputOp->execute();
	rec = leftTable->getEmptyRecord();
	iter = leftTable->getIteratorAlt();
	cnt = 0;
	cout << "Print first " << numRecordsToPrint << " records of left table: " << endl;
	while (iter->advance()) {
		iter->getCurrent(rec);
		cout << cnt << ": " << rec << endl;
		cnt++;
		if (cnt > numRecordsToPrint) {
			break;
		}
	}

	MyDB_TableReaderWriterPtr rightTable = rightInputOp->execute();
	rec = rightTable->getEmptyRecord();
	iter = rightTable->getIteratorAlt();
	cnt = 0;
	cout << "Print first " << numRecordsToPrint << " records of right table: " << endl;
	while (iter->advance()) {
		iter->getCurrent(rec);
		cout << cnt << ": " << rec << endl;
		cnt++;
		if (cnt > numRecordsToPrint) {
			break;
		}
	}

	cout << "start table join ..." << endl;
	string finalSelectionPredicate = "";
	pair<string, string> equalityCheck;
	bool isFirst = true;
	for (ExprTreePtr exprTree : outputSelectionPredicate) {
		if (isFirst) {
			// initialize final selection predicate
			finalSelectionPredicate = exprTree->toString();
			// construct equality check
			string toCompare1 = exprTree->getLHS()->toString();
			string toCompare2 = exprTree->getRHS()->toString();
			string lhsAttr, rhsAttr;
			if (toCompare1.substr(0, toCompare1.find("_")) == leftTableAlias) {
				lhsAttr = toCompare1;
				rhsAttr = toCompare2;
			}
			else {
				lhsAttr = toCompare2;
				rhsAttr = toCompare1;
			}
			//cout << "equality check: (LHS)" << lhsAttr << ", (RHS)" << rhsAttr << endl;
			equalityCheck = make_pair(lhsAttr, rhsAttr);
			isFirst = false;
		}
		else {
			finalSelectionPredicate = "&& ( " + finalSelectionPredicate + ", " + exprTree->toString() + ")";
		}
	}
	//cout << "final selection predicate: " << endl;
	//cout << finalSelectionPredicate << endl;

	vector<string> projections;
	string projection;
	//cout << "projections: " << endl;
	for (ExprTreePtr exprTree : exprsToCompute) {
		projection = exprTree->toString();
		//cout << projection << endl;
		projections.push_back(projection);
	}


	// SortMergeJoin (MyDB_TableReaderWriterPtr leftInput, MyDB_TableReaderWriterPtr rightInput,
	// 	MyDB_TableReaderWriterPtr output, string finalSelectionPredicate, 
	// 	vector <string> projections,
	// 	pair <string, string> equalityCheck, string leftSelectionPredicate,
	// 	string rightSelectionPredicate);
	MyDB_TableReaderWriterPtr outputTable = make_shared<MyDB_TableReaderWriter>(outputSpec, leftTable->getBufferMgr());
	SortMergeJoin op(leftTable, rightTable, outputTable, finalSelectionPredicate, projections, equalityCheck, "bool[true]", "bool[true]");
	op.run();
	rec = outputTable->getEmptyRecord();
	iter = outputTable->getIteratorAlt();
	cnt = 0;
	cout << "Print first " << numRecordsToPrint << " records of the joined table: " << endl;
	while (iter->advance()) {
		iter->getCurrent(rec);
		cnt++;
		if (cnt <= numRecordsToPrint) {
			cout << cnt << ": " << rec << endl;
		}
	}
	cout << "Record count after LogicalJoin: " << cnt << ".\n";
	leftTable->getBufferMgr()->killTable(leftTable->getTable());
	rightTable->getBufferMgr()->killTable(rightTable->getTable());
	return outputTable;
}

// this costs the table scan returning the compute set of statistics for the output
pair <double, MyDB_StatsPtr> LogicalTableScan :: cost () {
	MyDB_StatsPtr returnVal = inputStats->costSelection (selectionPred);
	return make_pair (returnVal->getTupleCount (), returnVal);	
}

// fill this out!  This should heuristically choose whether to use a B+-Tree (if appropriate) or just a regular
// table scan, and then execute the table scan using a relational selection.  Note that a desirable optimization
// is to somehow set things up so that if a B+-Tree is NOT used, that the table scan does not actually do anything,
// and the selection predicate is handled at the level of the parent (by filtering, for example, the data that is
// input into a join)
MyDB_TableReaderWriterPtr LogicalTableScan :: execute () {
	cout << "start of LogicalTableScan.execute()" << endl;

	MyDB_RecordPtr rec = inputSpec->getEmptyRecord();
	MyDB_RecordIteratorAltPtr iter = inputSpec->getIteratorAlt();
	int cnt = 0;
	int numRecordsToPrint = 30;
	cout << "Print first " << numRecordsToPrint << " records of input table: " << endl;
	while (iter->advance()) {
		iter->getCurrent(rec);
		cout << cnt << ": " << rec << endl;
		cnt++;
		if (cnt > numRecordsToPrint) {
			break;
		}
	}
	
	/*
	MyDB_TableReaderWriterPtr inputSpec;
	MyDB_TablePtr outputSpec;
	MyDB_StatsPtr inputStats;
    vector <ExprTreePtr> selectionPred;
	vector <string> exprsToCompute;
	*/
	string predicate = "";
	bool isFirst = true;
	for (ExprTreePtr exp : this->selectionPred) {
		string selection = exp->toString();
		if (isFirst) {
			predicate = selection;
			isFirst = false;
		}
		else {
			predicate = "&& ( " + predicate + ", " + selection + ")";
		}
	}

	// debug
	//cout << "original predicate: " << endl;
	//cout << predicate << endl;

	vector<pair<string, MyDB_AttTypePtr>> tableAttrs = inputSpec->getTable()->getSchema()->getAtts();
	for (auto attr : tableAttrs) {
		string attrName = this->tableAlias + "_" + attr.first;
		size_t index = predicate.find(attrName);
		while (index != string::npos) {
			predicate.replace(index, attrName.length(), attr.first);
			index = predicate.find(attrName);
		}
	}

	// debug
	//cout << "modified predicate: " << endl;
	//cout << predicate << endl;
	cout << "projections: " << endl;
	for (string expr : exprsToCompute) {
		cout << expr << endl;
	}

	MyDB_TableReaderWriterPtr outputPtr = make_shared<MyDB_TableReaderWriter>(outputSpec, inputSpec->getBufferMgr());
	
	// RegularSelection (MyDB_TableReaderWriterPtr input, MyDB_TableReaderWriterPtr output,
	// 	string selectionPredicate, vector <string> projections);
	RegularSelection op(inputSpec, outputPtr, predicate, exprsToCompute);
	op.run();

	// print stats
	rec = outputPtr->getEmptyRecord();
	iter = outputPtr->getIteratorAlt();
	cnt = 0;
	cout << "Print first " << numRecordsToPrint << " records of the result table: " << endl;
	while (iter->advance()) {
		iter->getCurrent(rec);
		cnt++;
		if (cnt <= numRecordsToPrint) {
			cout << cnt << ": " << rec << endl;
		}
	}
	cout << "Record count after LogicalTableScan: " << cnt << ".\n";
	return outputPtr;
}

#endif
