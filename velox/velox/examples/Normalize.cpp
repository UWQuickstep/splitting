#include <iostream>
#include <vector>
#include <map>
#include <ctime>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/file.h>
#include <arrow/io/api.h>
#include <arrow/api.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>

#include "velox/common/base/Nulls.h"
#include "velox/core/QueryCtx.h"
#include "velox/vector/arrow/Bridge.h"

#include <folly/init/Init.h>
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/connectors/tpch/TpchConnectorSplit.h"
#include "velox/core/Expressions.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/tpch/gen/TpchGen.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec::test;
using namespace std;

unsigned long getTimeDiff(struct timespec start_time, struct timespec end_time) {
    return (unsigned long)((end_time.tv_sec - start_time.tv_sec)*1000000000 +
        double(end_time.tv_nsec - start_time.tv_nsec));
    return 0;
}

class LoadCSVtoVelox : public VectorTestBase {
public:
	LoadCSVtoVelox() {}
	~LoadCSVtoVelox() {}
	RowVectorPtr load(string filename);
};

class StoreVeloxToCSV : public VectorTestBase {
public:
	StoreVeloxToCSV() {}
	~StoreVeloxToCSV() {}
	void store(vector<RowVectorPtr>&, RowVectorPtr&);
	void _store_single_row_vector(RowVectorPtr&, string);
	ulong velox_to_arrow_time_ns = 0;
	ulong arrow_to_csv_time_ns = 0;
};

class CreateColumnGroupings : public VectorTestBase {
public:
	CreateColumnGroupings(RowVectorPtr t) {
		// Register Presto scalar functions.
		functions::prestosql::registerAllScalarFunctions();

		// Register Presto aggregate functions.
		aggregate::prestosql::registerAllAggregateFunctions();

		// Register type resolver with DuckDB SQL parser.
		parse::registerTypeResolver();

		// Table being split
		this->_table = t;
	}
	~CreateColumnGroupings() {}
	void compute(vector<vector<int64_t>>&, vector<int64_t>&);

private:
	RowVectorPtr _table;
	void _get_sorted_col_order(vector<int64_t>&, vector<int64_t>&);
};

class Split : public VectorTestBase {
public:
	Split (RowVectorPtr t) {
		// Register Presto scalar functions.
		functions::prestosql::registerAllScalarFunctions();

		// Register Presto aggregate functions.
		aggregate::prestosql::registerAllAggregateFunctions();

		// Register type resolver with DuckDB SQL parser.
		parse::registerTypeResolver();

		// Table being split
		this->_table = t;
	}

	/// Parse SQL expression into a typed expression tree using DuckDB SQL parser.
	core::TypedExprPtr parseExpression(
			const std::string& text,
			const RowTypePtr& rowType) {
		parse::ParseOptions options;
		auto untyped = parse::parseExpr(text, options);
		return core::Expressions::inferTypes(untyped, rowType, execCtx_->pool());
	}

	/// Compile typed expression tree into an executable ExprSet.
	std::unique_ptr<exec::ExprSet> compileExpression(
			const std::string& expr,
			const RowTypePtr& rowType) {
		std::vector<core::TypedExprPtr> expressions = {
				parseExpression(expr, rowType)};
		return std::make_unique<exec::ExprSet>(
				std::move(expressions), execCtx_.get());
	}

	/// Evaluate an expression on one batch of data.
	VectorPtr evaluate(exec::ExprSet& exprSet, const RowVectorPtr& input) {
		exec::EvalCtx context(execCtx_.get(), &exprSet, input.get());

		SelectivityVector rows(input->size());
		std::vector<VectorPtr> result(1);
		exprSet.eval(rows, context, result);
		return result[0];
	}

	std::shared_ptr<folly::Executor> executor_{
			std::make_shared<folly::CPUThreadPoolExecutor>(
					std::thread::hardware_concurrency())};
	std::shared_ptr<core::QueryCtx> queryCtx_{
			std::make_shared<core::QueryCtx>(executor_.get())};
	std::unique_ptr<core::ExecCtx> execCtx_{
			std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get())};

	~Split() {}
	void compute(vector<vector<int64_t>>&, vector<int64_t>&,
		vector<RowVectorPtr>&, RowVectorPtr&, map<string, string>&);
	void compute_from_normalized_schema(vector<vector<string>>&, vector<RowVectorPtr>&);

private:
	RowVectorPtr _table;
};

RowVectorPtr LoadCSVtoVelox::load(string filename) {
	// STEP 0: Load file to Velox via Arrow
	// Load the file a first time
	struct timespec startTime, endTime;
	clock_gettime(CLOCK_MONOTONIC, &(startTime));

	bool needs_reloading = false;
	auto f = arrow::io::ReadableFile::Open(filename);
	shared_ptr<arrow::io::RandomAccessFile> input = f.ValueOrDie();

	arrow::io::IOContext io_context = arrow::io::default_io_context();
	
	auto read_options = arrow::csv::ReadOptions::Defaults();
	auto parse_options = arrow::csv::ParseOptions::Defaults();
	auto convert_options = arrow::csv::ConvertOptions::Defaults();

	// Instantiate TableReader from input stream and options
	auto maybe_reader =
	arrow::csv::TableReader::Make(io_context,
									input,
									read_options,
									parse_options,
									convert_options);

	shared_ptr<arrow::csv::TableReader> reader = *maybe_reader;
	if (!maybe_reader.ok()) {
		cout << "ERROR initializing Arrow CSV reader" << endl;
		exit(1);
	}
	auto maybe_table = reader->Read();
	if (!maybe_table.ok()) {
		cout << "ERROR reading table into Arrow" << endl;
		exit(2);
	}
	shared_ptr<arrow::Table> table = *maybe_table;
	int i = 0;
	auto col_names_from_csv = table->ColumnNames();
	// Check if any of the columns are of tss type
	for (const auto& column: table->columns()) {
		shared_ptr<arrow::Array> chunk = column->chunk(0);
		ArrowArray array;
		ArrowSchema schema;
		arrow::ExportArray(*chunk, &array, &schema);
		// // cout << "Column name: " << col_names_from_csv[i] << " Format type: " << schema.format << endl;

		// Arrow CSV loader loads into the following datatypes (see CDataInterfact.rst)
		// n : null
		// l : int64
		// g : float64
		// u/U : utf-8 string
		// z/Z : large binary
		// tdD : date32
		// tts : time32
		// tss: : timestamp[s]
		// tsn: : timestamp[ns]
		// We want to parse all the types as strings
		// First character of schema format string
		char schema_format = string(schema.format)[0];
		if (schema_format == 'z' || schema_format == 'Z') {
			cout << "ERROR: Binary column " << col_names_from_csv[i] <<
				" in data, not currently supported" << endl;
			exit(1);
		} else {
			// everything else is a string
			convert_options.column_types[col_names_from_csv[i]] = ::arrow::utf8();
			needs_reloading = true;
		}
		i += 1;
	}
	// // cout << endl;

	if (needs_reloading) {
		// Read the whole file when reading a second time
		f = arrow::io::ReadableFile::Open(filename);
		input = f.ValueOrDie();
		io_context = arrow::io::default_io_context();
		read_options = arrow::csv::ReadOptions::Defaults();
		maybe_reader =
		arrow::csv::TableReader::Make(io_context,
										input,
										read_options,
										parse_options,
										convert_options);

		reader = *maybe_reader;
		if (!maybe_reader.ok()) {
			cout << "ERROR initializing Arrow CSV reader" << endl;
			exit(1);
		}
		maybe_table = reader->Read();
		if (!maybe_table.ok()) {
			cout << "ERROR reading table into Arrow" << endl;
			exit(2);
		}
		table = *maybe_table;
	}
	shared_ptr<arrow::Table> combined = table->CombineChunks().ValueOrDie();

	clock_gettime(CLOCK_MONOTONIC, &(endTime));
	auto timeDiff = getTimeDiff(startTime, endTime);
	// cout << "Time for loading CSV into Arrow: " << timeDiff << " ns" << endl;
	clock_gettime(CLOCK_MONOTONIC, &(startTime));

	// Convert arrow::Table to velox vectors
	// Have to convert each individual array (column) in the table
	// to a velox vector, and convert the collection of vectors into a velox table
	vector<VectorPtr> velox_vectors;
	int c = 0;
	i = 0;
	col_names_from_csv = combined->ColumnNames();
	for (const auto& column : combined->columns()) {
		// Ensure that there is only one chunk per column
		if (column->num_chunks() > 1) {
			// cout << "More than one chunk: " << column->num_chunks() << endl;
		}

		// Convert arrow::Array to ArrowArray type defined in Arrow ABI
		ArrowArray array;
		ArrowSchema schema;
		shared_ptr<arrow::Array> chunk = column->chunk(0);
		arrow::ExportArray(*chunk, &array, &schema);
		// cout << "Column name: " << col_names_from_csv[i] << " Format type: " << schema.format << endl;

		// Import ArrowArray to Velox BaseVector
		auto velox_vector = importFromArrowAsOwner(schema, array, pool());
		velox_vectors.push_back(velox_vector);
		c += 1;
		i += 1;

		// Release Arrow memory -- throwing SEG_FAULT
		// array.release(&array);
		// schema.release(&schema);
	}

	// Convert the vector of facebook::velox::BaseVector to a velox table
	auto velox_table = makeRowVector(col_names_from_csv, velox_vectors);
	auto num_rows = velox_table->size();
	auto num_cols = velox_table->children().size();
	// cout << "Number of rows: " << num_rows << endl;
	// cout << "Number of columns: " << num_cols << endl;
	// auto types = velox_table->type();
	// cout << "Types: " << endl;
	// for (int i=0; i<num_cols; i++) {
	// 	cout << "Column name: " << col_names_from_csv[i] << ":" << types->childAt(i)->toString() << endl;
	// }

	clock_gettime(CLOCK_MONOTONIC, &(endTime));
	timeDiff = getTimeDiff(startTime, endTime);
	// cout << "Time for porting from Arrow to Velox: " << timeDiff << " ns" << endl;
	return velox_table;
}

void CreateColumnGroupings::_get_sorted_col_order(
	vector<int64_t>& approx_counts, vector<int64_t>& sorted_order) {
	int num_cols = approx_counts.size();
	vector<int64_t> included (num_cols, 0);

	int min_count = 0;
	int max_count = 0;
	int col_index;
	int num_cols_processed = 0;

	for (int i=0; i<num_cols; i++) {
		if (approx_counts[i] > max_count) {
			max_count = approx_counts[i];
		}
	}

	// Just the naive O(n^2) solution
	while (num_cols_processed < num_cols) {
		min_count = max_count;
		col_index = num_cols;
		for (int i=0; i<num_cols; i++) {
			if (included[i]==0 && approx_counts[i] <= min_count) {
				min_count = approx_counts[i];
				col_index = i;
			}
		}
		sorted_order.push_back(col_index);
		included[col_index] = 1;
		num_cols_processed += 1;
	}
}

void CreateColumnGroupings::compute(
	vector<vector<int64_t>> &col_groups, vector<int64_t> &fact) {
	// STEP 1:
	// Perform Approximate COUNT DISTINCT and measure the TOTAL SIZE in bytes
	int num_cols = _table->children().size();
	for (int i=0; i < num_cols; i++) {
		vector<int64_t> temp {i};
		col_groups.push_back(temp);
	}
	return;
	int num_rows = 0;
	vector<int64_t> approx_counts;
	vector<int64_t> total_sizes;
	vector<int64_t> max_sizes;
	vector<int64_t> sorted_order;

	vector<string> aggregates;
	aggregates.push_back("count(*)");
	for (int i=0; i<num_cols; i++) {
		auto col = _table->childAt(i);
		if (col->typeKind() == TypeKind::VARBINARY) {
			aggregates.push_back(
				string("count(c") +
				to_string(i) +
				string(") as ad") +
				to_string(i));
		} else {
			aggregates.push_back(
				string("approx_distinct(c") +
				to_string(i) +
				string(") as ad") +
				to_string(i));
		}
	}
	for (int i=0; i<num_cols; i++) {
		aggregates.push_back(
			string("total_size(c") +
			to_string(i) +
			string(") as ts") +
			to_string(i));
	}
	for (int i=0; i<num_cols; i++) {
		aggregates.push_back(
			string("max_data_size_for_stats(c") +
			to_string(i) +
			string(") as ts") +
			to_string(i));
	}
	auto plan = PlanBuilder()
					.values({_table})
					.singleAggregation(
						{},
						aggregates)
					.planNode();

	// Collect results -- number of rows, approx count distinct, and total sizes of each column
	auto results = AssertQueryBuilder(plan).copyResults(pool());
	num_rows = stoi(results->childAt(0)->toString(0));
	// cout << "Total no. of rows: " << num_rows << endl;

	cout << endl << "Approx. count distinct:" << endl;
	for (int i=0; i<num_cols; i++) {
		approx_counts.push_back(stoi(results->childAt(i+1)->toString(0)));
		cout << "Column " << i << ": " << approx_counts.back() << endl;
	}

	cout << endl << "Total size:" << endl;
	for (int i=0; i<num_cols; i++) {
		total_sizes.push_back(stoi(results->childAt(num_cols+i+1)->toString(0)));
		cout << "Column " << i << ": " << total_sizes.back() << endl;
	}

	cout << endl << "Max size:" << endl;
	for (int i=0; i<num_cols; i++) {
		max_sizes.push_back(stoi(results->childAt(2*num_cols+i+1)->toString(0)));
		cout << "Column " << i << ": " << max_sizes.back() << endl;
	}

	// STEP 2: Sort columns by approx counts
	_get_sorted_col_order(approx_counts, sorted_order);
	cout << endl << "Sorted order: ";
	for (int i=0; i<num_cols; i++) {
		cout << sorted_order[i] << " ";
	}
	cout << endl;

	// STEP 3: Compute column groupings
	vector<int64_t> col_group;
	ulong estimated_cardinality;
	float estimated_tuple_size;
	float estimated_space;
	int64_t actual_space;
	int64_t col_id;

	col_group.push_back(sorted_order[0]);
	for (int i=1; i<num_cols; i++) {
		// Try adding column i in the sorted order to the current col_group
		col_id = sorted_order[i];
		estimated_cardinality = approx_counts[col_id];
		// estimated_tuple_size = float(total_sizes[col_id])/num_rows;
		estimated_tuple_size = max_sizes[col_id];
		actual_space = total_sizes[col_id];
		for (int j=0; j<col_group.size(); j++) {
			col_id = col_group[j];
			estimated_cardinality *= approx_counts[col_id];
			// estimated_tuple_size += float(total_sizes[col_id])/num_rows;
			estimated_tuple_size += max_sizes[col_id];
			actual_space += total_sizes[col_id];
		}
		// 8-byte keys
		estimated_space = estimated_cardinality*estimated_tuple_size + (num_rows+estimated_cardinality)*8;
		if (estimated_space < actual_space) {
			col_group.push_back(sorted_order[i]);
		} else {
			vector<int64_t> temp;
			temp.swap(col_group);
			if (temp.size() == 1) {
				// Evaluate if dictionary encoding makes sense for this column
				col_id = temp[0];
				estimated_cardinality = approx_counts[col_id];
				estimated_tuple_size = float(total_sizes[col_id])/num_rows;
				actual_space = total_sizes[col_id];
				estimated_space = estimated_cardinality*estimated_tuple_size + (num_rows+estimated_cardinality)*8;
				if (estimated_space < actual_space) {
					col_groups.push_back(temp);
				} else {
					fact.push_back(col_id);
				}
			} else {
				col_groups.push_back(temp);
			}
			col_group.push_back(sorted_order[i]);
		}
	}

	if (col_group.size() > 1) {
		col_groups.push_back(col_group);
	} else if (col_group.size() == 1) {
		// Evaluate if dictionary encoding makes sense for this column
		col_id = col_group[0];
		estimated_cardinality = approx_counts[col_id];
		estimated_tuple_size = float(total_sizes[col_id])/num_rows;
		actual_space = total_sizes[col_id];
		estimated_space = estimated_cardinality*estimated_tuple_size + (num_rows+estimated_cardinality)*8;
		if (estimated_space < actual_space) {
			col_groups.push_back(col_group);
		} else {
			fact.push_back(col_id);
		}
	}

	for (int i=0; i<col_groups.size(); i++) {
		cout << "Group " << i << ":";
		for (int j=0; j<col_groups[i].size(); j++) {
			cout << " " << col_groups[i][j];
		}
		cout << endl;
	}

	cout << "Fact: ";
	for (int i=0; i<fact.size(); i++) {
		cout << fact[i] << " ";
	}
	cout << endl;
}

// USES WINDOW FUNCTION AND AGGREGATION
void Split::compute(
	vector<vector<int64_t>> &col_groups, vector<int64_t> &fact,
	vector<RowVectorPtr> &dim_tables, RowVectorPtr &fact_table,
	map<string, string> &col_name_mappings) {
	// STEP 4: Split the table into star schema

	// Local variables
	RowVectorPtr fact_table_local;
	vector<RowVectorPtr> dim_tables_local;
	vector<vector<string>> col_groups_names;
	vector<vector<string>> orig_names_dims;
	vector<string> orig_names_fact;
	for (auto col_group : col_groups) {
		vector<string> col_names;
		for (auto col_no : col_group) {
			string col_name = string("c") + to_string(col_no);
			col_names.push_back(col_name);
		}
		col_groups_names.push_back(col_names);
	}

	// Iterate over all the col groups to generate dimension tables and fact table
	auto group_no = 0;
	fact_table_local = _table;

	// Fact table col names are updated as we update the fact table
	vector<string> fact_table_col_names;
	auto num_cols = fact_table_local->children().size();
	for (auto i=0; i<num_cols; i++) {
		fact_table_col_names.push_back(string("c") + to_string(i));
	}
	for (auto dim_col_names : col_groups_names) {
		// Add a column to the fact table
		vector<int64_t> keys;
		auto num_rows = fact_table_local->size();
		for (auto i=0; i<num_rows; i++) {
			keys.push_back(i);
		}
		auto key_col = makeFlatVector<int64_t>(keys);
		vector<VectorPtr> cols;
		for (auto col : fact_table_local->children()) {
			cols.push_back(col);
		}
		cols.push_back(key_col);
		vector<string> temp_table_col_names(fact_table_col_names);
		temp_table_col_names.push_back("key");
		auto temp_table = makeRowVector(temp_table_col_names, cols);

		// Generate the dimension table
		string aggregate = "min(key) as p" + to_string(group_no);
		auto plan = PlanBuilder()
					.values({temp_table})
					.aggregation(
						dim_col_names,
						{aggregate},
						{},
						core::AggregationNode::Step::kPartial,
						false)
					.planNode();
		auto results = AssertQueryBuilder(plan).copyResults(pool());
		// // cout << results->toString() << endl;
		// // cout << results->toString(1, 10) << endl << endl;

		// Keep track of original names of the columns in the dimension table
		// for reassigning the original names at the end
		vector<string> orig_names_dim;
		for (auto col_name : dim_col_names) {
			orig_names_dim.push_back(col_name_mappings[col_name]);
		}
		orig_names_dim.push_back(string("p") + to_string(group_no));
		orig_names_dims.push_back(orig_names_dim);
		// // cout << results->toString(1, 10) << endl << endl;
		dim_tables_local.push_back(results);

		// Generate the fact table

		// Generate the window clause
		string window_clause = string("min(key) OVER (PARTITION BY ");
		auto j = 0;
		for (auto dim_col_name : dim_col_names) {
			if (j != 0) window_clause += string(", ");
			window_clause += dim_col_name;
			j += 1;
		}
		window_clause += " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as p" + to_string(group_no);

		// Generate the fact table column names to be projected out
		vector<string> project_col_names;
		for (auto col_name : fact_table_col_names) {
			if (find(dim_col_names.begin(), dim_col_names.end(), col_name) != dim_col_names.end())
				// A dimension column
				continue;
			project_col_names.push_back(col_name);
		}
		project_col_names.push_back(string("p") + to_string(group_no));
		fact_table_col_names = project_col_names;

		plan = PlanBuilder()
				.values({temp_table})
				.window({window_clause})
				.project(fact_table_col_names)
				.planNode();
		fact_table_local = AssertQueryBuilder(plan).copyResults(pool());
		temp_table = nullptr;

		group_no++;
	}

	for (auto col_name : fact_table_col_names) {
		if (col_name[0] == 'p') {
			orig_names_fact.push_back(col_name);
		} else {
			orig_names_fact.push_back(col_name_mappings[col_name]);
		}
	}
	fact_table = makeRowVector(orig_names_fact, fact_table_local->children());
	// cout << orig_names_fact.size() << " " << fact_table_local->children().size() << endl;

	group_no = 0;
	for (auto orig_names_dim : orig_names_dims) {
		auto results = makeRowVector(orig_names_dim, dim_tables_local[group_no]->children());
		dim_tables.push_back(results);
		group_no += 1;
	}

	// cout << fact_table->toString() << endl;
	// cout << fact_table->toString(0, 10) << endl << endl;
}

void Split::compute_from_normalized_schema(vector<vector<string>> &schema, vector<RowVectorPtr> &output) {
	for (auto table_cols : schema) {
		auto plan = PlanBuilder()
					.values({_table})
					.aggregation(
						table_cols,
						{},
						{},
						core::AggregationNode::Step::kPartial,
						false)
					.planNode();
		auto results = AssertQueryBuilder(plan).copyResults(pool());
		output.push_back(results);
	}
}

void StoreVeloxToCSV::_store_single_row_vector(RowVectorPtr& velox_table, string filename) {
	struct timespec startTime, endTime;
	clock_gettime(CLOCK_MONOTONIC, &(startTime));

	ArrowSchema schema;
	exportToArrow(velox_table, schema);
	auto arrow_schema = arrow::ImportSchema(&schema).ValueOrDie();

	// For some reason this has to be done twice
	exportToArrow(velox_table, schema);
	vector<std::shared_ptr<arrow::Array>> arrow_arrays;
	auto i = 0;
	for (auto child : velox_table->children()) {
		ArrowArray array;
		exportToArrow(child, array, pool());
		auto arr = arrow::ImportArray(&array, schema.children[i]).ValueOrDie();
		// arrow_arrays.push_back(arrow::ImportArray(&array, _type).ValueOrDie());
		arrow_arrays.push_back(arr);
		i++;
	}

	auto arrow_table = arrow::Table::Make(arrow_schema, arrow_arrays);

	clock_gettime(CLOCK_MONOTONIC, &(endTime));
	auto timeDiff = getTimeDiff(startTime, endTime);
	velox_to_arrow_time_ns += timeDiff;
	clock_gettime(CLOCK_MONOTONIC, &(startTime));

	auto outstream = arrow::io::FileOutputStream::Open(filename).ValueOrDie();
	arrow::csv::WriteCSV(*arrow_table, arrow::csv::WriteOptions::Defaults(), outstream.get());

	clock_gettime(CLOCK_MONOTONIC, &(endTime));
	timeDiff = getTimeDiff(startTime, endTime);
	arrow_to_csv_time_ns += timeDiff;
}

void StoreVeloxToCSV::store(vector<RowVectorPtr> &dim_tables, RowVectorPtr &fact_table) {
	// Store fact table
	_store_single_row_vector(fact_table, "fact.csv");
	auto i = 0;
	for (auto table : dim_tables) {
		_store_single_row_vector(table, string("dim")+to_string(i)+string(".csv"));
		i++;
	}
	// cout << "Time to port velox to arrow: " << velox_to_arrow_time_ns << " ns" << endl;
	// cout << "Time to store to CSV: " << arrow_to_csv_time_ns << " ns" << endl;
}

int main(int argc, char** argv) {
	if (argc != 2) {
		// cout << "Please include data file name in argument" << endl;
		exit(1);
	}
	folly::init(&argc, &argv, false);

	// Load the CSV file into Velox via Arrow
	LoadCSVtoVelox loader;
	string path_to_file(argv[1]);
	struct timespec startTime, endTime;
	auto table = loader.load(path_to_file);

	clock_gettime(CLOCK_MONOTONIC, &(startTime));

	// // Compute column groupings for splitting
	// CreateColumnGroupings groups(table);
	// vector<vector<int64_t>> col_groups;
	// vector<int64_t> fact_cols;
	// groups.compute(col_groups, fact_cols);
	// if (col_groups.size() == 0) {
	// 	cout << "TABLE NOT SPLIT, RETAIN ORIGINAL CSV" << endl;
	// 	return 0;
	// }

	vector<vector<string>> schema = 
	{
		{"id_odsp", "id_event", "text", "sort_order", "time"},
		{"id_odsp", "player2", "text", "time"},
		{"side", "id_odsp", "player", "player_out", "text"},
		{"event_type", "text", "time"},
		{"assist_method", "bodypart", "is_goal", "situation", "fast_break", "location", "shot_place", "shot_outcome", "text", "event_type2"},
		{"id_odsp", "player_in", "player_out"},
		{"side", "opponent", "id_odsp", "event_team"}
	};

	// // Split the table into star schema
	// Split split(table);
	// vector<RowVectorPtr> dim_tables;
	// RowVectorPtr fact_table;
	// split.compute(col_groups, fact_cols, dim_tables, fact_table, col_name_mappings);

	Split split(table);
	vector<RowVectorPtr> output;
	split.compute_from_normalized_schema(schema, output);

	clock_gettime(CLOCK_MONOTONIC, &(endTime));
	auto timeDiff = getTimeDiff(startTime, endTime);
	cout << "Time for splitting: " << timeDiff << " ns" << endl;

	// Convert back to arrow and store as CSV
	StoreVeloxToCSV store = StoreVeloxToCSV();
	int i = 0;
	for (auto table : output) {
		store._store_single_row_vector(table, to_string(i) + string(".csv"));
		i += 1;
	}

	return 0;
}