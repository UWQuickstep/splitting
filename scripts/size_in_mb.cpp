#include <iostream>
#include <string>

using namespace std;

int main(int argc, char** argv) {
	if (argc != 2) {
		cout << "Usage is ./mb_size {size_in_bytes}" << endl;
	}
	float MB = 1000000.0;
	int size_in_bytes = stoi(argv[1]);
	cout << size_in_bytes/MB << " MB" << endl;
	return 0;
}
