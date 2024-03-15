
	// USES WINDOW FUNCTIONS
	void _compute_v2(vector<vector<int64_t>>&, vector<int64_t>&,
		vector<RowVectorPtr>&, RowVectorPtr&, map<string, string>&);

// USES WINDOW FUNCTION AND AGGREGATION
void Decompose::_compute_v2(
	vector<vector<int64_t>> &col_groups, vector<int64_t> &fact,
	vector<RowVectorPtr> &dim_tables, RowVectorPtr &fact_table,
	map<string, string> &col_name_mappings) {
	// STEP 4: Decompose the table into star schema

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