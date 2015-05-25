//    mwa_converter.cc: convert MWA measurementsets from TiledStMan to AdiosStMan 
//
//    (c) University of Western Australia
//    International Centre of Radio Astronomy Research
//    M468, 35 Stirling Hwy
//    Crawley, Perth WA 6009
//    Australia
//
//    This library is free software: you can redistribute it and/or
//    modify it under the terms of the GNU General Public License as published
//    by the Free Software Foundation, either version 3 of the License, or
//    (at your option) any later version.
//   
//    This library is distributed in the hope that it will be useful,
//    but WITHOUT ANY WARRANTY; without even the implied warranty of
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//    GNU General Public License for more details.
//   
//    You should have received a copy of the GNU General Public License along
//    with this library. If not, see <http://www.gnu.org/licenses/>.
//
//    Any bugs, questions, concerns and/or suggestions please email to
//    jason.wang@icrar.org

#include <tables/Tables/TableDesc.h>
#include <tables/Tables/SetupNewTab.h>
#include <tables/Tables/ScaColDesc.h>
#include <tables/Tables/ScalarColumn.h>
#include <casacore/tables/Tables/ArrColDesc.h>
#include <casacore/tables/Tables/ArrayColumn.h>
#include "../AdiosStMan.h"
#include <casa/namespace.h>
#include "tictak.h"

uInt TotalRows = 0;
uInt Rows = 0;
Table *write_table = NULL;
int mpiRank, mpiSize;

ROScalarColumn<bool> *FLAG_ROW_col;
ROScalarColumn<int> *ANTENNA1_col;
ROScalarColumn<int> *ANTENNA2_col;
ROScalarColumn<int> *ARRAY_ID_col;
ROScalarColumn<int> *DATA_DESC_ID_col;
ROScalarColumn<int> *FEED1_col;
ROScalarColumn<int> *FEED2_col;
ROScalarColumn<int> *FIELD_ID_col;
ROScalarColumn<int> *PROCESSOR_ID_col;
ROScalarColumn<int> *SCAN_NUMBER_col;
ROScalarColumn<int> *STATE_ID_col;
ROScalarColumn<double> *INTERVAL_col;
ROScalarColumn<double> *EXPOSURE_col;
ROScalarColumn<double> *TIME_col;
ROScalarColumn<double> *TIME_CENTROID_col;

ROArrayColumn<Double> *UVW_col;
ROArrayColumn<bool> *FLAG_col;
ROArrayColumn<float> *WEIGHT_col;
ROArrayColumn<float> *SIGMA_col;
ROArrayColumn<Complex> *DATA_col;
ROArrayColumn<float> *WEIGHT_SPECTRUM_col;
ROArrayColumn<Complex> *CORRECTED_DATA_col;

void write_rows(){

	uInt rows;
	if(Rows == 0) rows = TotalRows;
	else rows = Rows;
	

	for(int i=mpiRank; i<rows; i+=mpiSize){
		// FLAG_ROW column
		ScalarColumn<bool> FLAG_ROW_col_new (*write_table, "FLAG_ROW");
		FLAG_ROW_col_new.put(i, FLAG_ROW_col->get(i));
		// ANTENNA1 column
		ScalarColumn<int> ANTENNA1_col_new (*write_table, "ANTENNA1");
		ANTENNA1_col_new.put(i, ANTENNA1_col->get(i));
		// ANTENNA2 column
		ScalarColumn<int> ANTENNA2_col_new (*write_table, "ANTENNA2");
		ANTENNA2_col_new.put(i, ANTENNA2_col->get(i));
		// ARRAY_ID column
		ScalarColumn<int> ARRAY_ID_col_new (*write_table, "ARRAY_ID");
		ARRAY_ID_col_new.put(i, ARRAY_ID_col->get(i));
		// DATA_DESC_ID column
		ScalarColumn<int> DATA_DESC_ID_col_new (*write_table, "DATA_DESC_ID");
		DATA_DESC_ID_col_new.put(i, DATA_DESC_ID_col->get(i));
		// EXPOSURE column
		ScalarColumn<double> EXPOSURE_col_new (*write_table, "EXPOSURE");
		EXPOSURE_col_new.put(i, EXPOSURE_col->get(i));
		// FEED1 column
		ScalarColumn<int> FEED1_col_new (*write_table, "FEED1");
		FEED1_col_new.put(i, FEED1_col->get(i));
		// FEED2 column
		ScalarColumn<int> FEED2_col_new (*write_table, "FEED2");
		FEED2_col_new.put(i, FEED2_col->get(i));
		// FIELD_ID column
		ScalarColumn<int> FIELD_ID_col_new (*write_table, "FIELD_ID");
		FIELD_ID_col_new.put(i, FIELD_ID_col->get(i));
		// INTERVAL column
		ScalarColumn<double> INTERVAL_col_new (*write_table, "INTERVAL");
		INTERVAL_col_new.put(i, INTERVAL_col->get(i));
		// PROCESSOR_ID column
		ScalarColumn<int> PROCESSOR_ID_col_new (*write_table, "PROCESSOR_ID");
		PROCESSOR_ID_col_new.put(i, PROCESSOR_ID_col->get(i));
		// SCAN_NUMBER column
		ScalarColumn<int> SCAN_NUMBER_col_new (*write_table, "SCAN_NUMBER");
		SCAN_NUMBER_col_new.put(i, SCAN_NUMBER_col->get(i));
		// STATE_ID column
		ScalarColumn<int> STATE_ID_col_new (*write_table, "STATE_ID");
		STATE_ID_col_new.put(i, STATE_ID_col->get(i));
		// TIME column
		ScalarColumn<double> TIME_col_new (*write_table, "TIME");
		TIME_col_new.put(i, TIME_col->get(i));
		// TIME_CENTROID column
		ScalarColumn<double> TIME_CENTROID_col_new (*write_table, "TIME_CENTROID");
		TIME_CENTROID_col_new.put(i, TIME_CENTROID_col->get(i));
		// UVW column
		ArrayColumn<double> UVW_col_new (*write_table, "UVW");
		UVW_col_new.put(i, UVW_col->get(i));
		// FLAG column
		ArrayColumn<bool> FLAG_col_new (*write_table, "FLAG");
		FLAG_col_new.put(i, FLAG_col->get(i));
		// WEIGHT column
		ArrayColumn<float> WEIGHT_col_new (*write_table, "WEIGHT");
		WEIGHT_col_new.put(i, WEIGHT_col->get(i));
		// SIGMA column
		ArrayColumn<float> SIGMA_col_new (*write_table, "SIGMA");
		SIGMA_col_new.put(i, SIGMA_col->get(i));
		// WEIGHT_SPECTRUM column
		ArrayColumn<float> WEIGHT_SPECTRUM_col_new (*write_table, "WEIGHT_SPECTRUM");
		WEIGHT_SPECTRUM_col_new.put(i, WEIGHT_SPECTRUM_col->get(i));
		// CORRECTED_DATA column
		ArrayColumn<Complex> CORRECTED_DATA_col_new (*write_table, "CORRECTED_DATA");
		CORRECTED_DATA_col_new.put(i, CORRECTED_DATA_col->get(i));
		// DATA column
		ArrayColumn<Complex> DATA_col_new (*write_table, "DATA");
		DATA_col_new.put(i, DATA_col->get(i));

		if (i%100 == 0)
			cout << i << " rows finished, from Rank" << mpiRank << endl;

	}
}

void write_columns(){
	// FLAG_ROW column
	ScalarColumn<bool> FLAG_ROW_col_new (*write_table, "FLAG_ROW");
	FLAG_ROW_col_new.putColumn(FLAG_ROW_col->getColumn());
	cout << "FLAG_ROW column completed" << endl;
	// ANTENNA1 column
	ScalarColumn<int> ANTENNA1_col_new (*write_table, "ANTENNA1");
	ANTENNA1_col_new.putColumn(ANTENNA1_col->getColumn());
	cout << "ANTENNA1 column completed" << endl;
	// ANTENNA2 column
	ScalarColumn<int> ANTENNA2_col_new (*write_table, "ANTENNA2");
	ANTENNA2_col_new.putColumn(ANTENNA2_col->getColumn());
	cout << "ANTENNA2 column completed" << endl;
	// ARRAY_ID column
	ScalarColumn<int> ARRAY_ID_col_new (*write_table, "ARRAY_ID");
	ARRAY_ID_col_new.putColumn(ARRAY_ID_col->getColumn());
	cout << "ARRAY_ID column completed" << endl;
	// DATA_DESC_ID column
	ScalarColumn<int> DATA_DESC_ID_col_new (*write_table, "DATA_DESC_ID");
	DATA_DESC_ID_col_new.putColumn(DATA_DESC_ID_col->getColumn());
	cout << "DATA_DESC_ID column completed" << endl;
	// EXPOSURE column
	ScalarColumn<double> EXPOSURE_col_new (*write_table, "EXPOSURE");
	EXPOSURE_col_new.putColumn(EXPOSURE_col->getColumn());
	cout << "EXPOSURE column completed" << endl;
	// FEED1 column
	ScalarColumn<int> FEED1_col_new (*write_table, "FEED1");
	FEED1_col_new.putColumn(FEED1_col->getColumn());
	cout << "FEED1 column completed" << endl;
	// FEED2 column
	ScalarColumn<int> FEED2_col_new (*write_table, "FEED2");
	FEED2_col_new.putColumn(FEED2_col->getColumn());
	cout << "FEED2 column completed" << endl;
	// FIELD_ID column
	ScalarColumn<int> FIELD_ID_col_new (*write_table, "FIELD_ID");
	FIELD_ID_col_new.putColumn(FIELD_ID_col->getColumn());
	cout << "FIELD_ID column completed" << endl;
	// INTERVAL column
	ScalarColumn<double> INTERVAL_col_new (*write_table, "INTERVAL");
	INTERVAL_col_new.putColumn(INTERVAL_col->getColumn());
	cout << "INTERVAL column completed" << endl;
	// PROCESSOR_ID column
	ScalarColumn<int> PROCESSOR_ID_col_new (*write_table, "PROCESSOR_ID");
	PROCESSOR_ID_col_new.putColumn(PROCESSOR_ID_col->getColumn());
	cout << "PROCESSOR_ID column completed" << endl;
	// SCAN_NUMBER column
	ScalarColumn<int> SCAN_NUMBER_col_new (*write_table, "SCAN_NUMBER");
	SCAN_NUMBER_col_new.putColumn(SCAN_NUMBER_col->getColumn());
	cout << "SCAN_NUMBER column completed" << endl;
	// STATE_ID column
	ScalarColumn<int> STATE_ID_col_new (*write_table, "STATE_ID");
	STATE_ID_col_new.putColumn(STATE_ID_col->getColumn());
	cout << "STATE_ID column completed" << endl;
	// TIME column
	ScalarColumn<double> TIME_col_new (*write_table, "TIME");
	TIME_col_new.putColumn(TIME_col->getColumn());
	cout << "TIME column completed" << endl;
	// TIME_CENTROID column
	ScalarColumn<double> TIME_CENTROID_col_new (*write_table, "TIME_CENTROID");
	TIME_CENTROID_col_new.putColumn(TIME_CENTROID_col->getColumn());
	cout << "TIME_CENTROID column completed" << endl;
	// UVW column
	ArrayColumn<double> UVW_col_new (*write_table, "UVW");
	UVW_col_new.putColumn(UVW_col->getColumn());
	cout << "UVW column completed" << endl;
	// FLAG column
	ArrayColumn<bool> FLAG_col_new (*write_table, "FLAG");
	FLAG_col_new.putColumn(FLAG_col->getColumn());
	cout << "FLAG column completed" << endl;
	// WEIGHT column
	ArrayColumn<float> WEIGHT_col_new (*write_table, "WEIGHT");
	WEIGHT_col_new.putColumn(WEIGHT_col->getColumn());
	cout << "WEIGHT column completed" << endl;
	// SIGMA column
	ArrayColumn<float> SIGMA_col_new (*write_table, "SIGMA");
	SIGMA_col_new.putColumn(SIGMA_col->getColumn());
	cout << "SIGMA column completed" << endl;
	// WEIGHT_SPECTRUM column
	ArrayColumn<float> WEIGHT_SPECTRUM_col_new (*write_table, "WEIGHT_SPECTRUM");
	for(int i=0; i<TotalRows; i++){
		WEIGHT_SPECTRUM_col_new.put(i, WEIGHT_SPECTRUM_col->get(i));
	}
	cout << "WEIGHT_SPECTRUM column completed" << endl;
	// CORRECTED_DATA column
	ArrayColumn<Complex> CORRECTED_DATA_col_new (*write_table, "CORRECTED_DATA");
	for(int i=0; i<TotalRows; i++){
		CORRECTED_DATA_col_new.put(i, CORRECTED_DATA_col->get(i));
	}
	cout << "CORRECTED_DATA column completed" << endl;
	// DATA column
	ArrayColumn<Complex> DATA_col_new (*write_table, "DATA");
	for(int i=0; i<TotalRows; i++){
		DATA_col_new.put(i, DATA_col->get(i));
	}
	cout << "DATA column completed" << endl;
}

int main (int argc, char **argv){

	tictak_add((char*)"begin", 0);

	string file_input="/scratch/jason/1067892840_tsm.ms";
	string file_output="/scratch/jason/1067892840_adiosA.ms";

	if (argc >= 3){
		file_input = argv[1];
		file_output = argv[2];
	}
	
	if (argc >= 4){
		Rows = atoi(argv[3]);
	}

	cout << "ms input = " << file_input << endl;
	cout << "ms output = " << file_output << endl;

	// ####### Storage Manager used to write output measurement set
	AdiosStMan stman;
//	stman.setStManColumnType(AdiosStMan::VAR);
	stman.setStManColumnType(AdiosStMan::ARRAY);

	MPI_Comm_rank(MPI_COMM_WORLD, &mpiRank);
	MPI_Comm_size(MPI_COMM_WORLD, &mpiSize);

	// ####### read init
	Table read_table(file_input);    
	TotalRows = read_table.nrow();

	// ####### write init
	TableDesc td("", "1", TableDesc::Scratch);

	// ####### column init for read & write

	// FLAG_ROW column
	FLAG_ROW_col = new ROScalarColumn<bool> (read_table, "FLAG_ROW");
	td.addColumn (ScalarColumnDesc<bool>("FLAG_ROW"));

	// ANTENNA1 column
	ANTENNA1_col = new ROScalarColumn<int> (read_table, "ANTENNA1");
	td.addColumn (ScalarColumnDesc<int>("ANTENNA1"));

	// ANTENNA2 column
	ANTENNA2_col = new ROScalarColumn<int> (read_table, "ANTENNA2");
	td.addColumn (ScalarColumnDesc<int>("ANTENNA2"));

	// ARRAY_ID column
	ARRAY_ID_col = new ROScalarColumn<int> (read_table, "ARRAY_ID");
	td.addColumn (ScalarColumnDesc<int>("ARRAY_ID"));

	// DATA_DESC_ID column
	DATA_DESC_ID_col = new ROScalarColumn<int> (read_table, "DATA_DESC_ID");
	td.addColumn (ScalarColumnDesc<int>("DATA_DESC_ID"));

	// EXPOSURE column
	EXPOSURE_col = new ROScalarColumn<double> (read_table, "EXPOSURE");
	td.addColumn (ScalarColumnDesc<double>("EXPOSURE"));

	// FEED1 column
	FEED1_col = new ROScalarColumn<int> (read_table, "FEED1");
	td.addColumn (ScalarColumnDesc<int>("FEED1"));

	// FEED2 column
	FEED2_col = new ROScalarColumn<int> (read_table, "FEED2");
	td.addColumn (ScalarColumnDesc<int>("FEED2"));

	// FIELD_ID column
	FIELD_ID_col = new ROScalarColumn<int> (read_table, "FIELD_ID");
	td.addColumn (ScalarColumnDesc<int>("FIELD_ID"));

	// INTERVAL column
	INTERVAL_col = new ROScalarColumn<double> (read_table, "INTERVAL");
	td.addColumn (ScalarColumnDesc<double>("INTERVAL"));

	// PROCESSOR_ID column
	PROCESSOR_ID_col = new ROScalarColumn<int> (read_table, "PROCESSOR_ID");
	td.addColumn (ScalarColumnDesc<int>("PROCESSOR_ID"));

	// SCAN_NUMBER column
	SCAN_NUMBER_col = new ROScalarColumn<int> (read_table, "SCAN_NUMBER");
	td.addColumn (ScalarColumnDesc<int>("SCAN_NUMBER"));

	// STATE_ID column
	STATE_ID_col = new ROScalarColumn<int> (read_table, "STATE_ID");
	td.addColumn (ScalarColumnDesc<int>("STATE_ID"));

	// TIME column
	TIME_col = new ROScalarColumn<double> (read_table, "TIME");
	td.addColumn (ScalarColumnDesc<double>("TIME"));

	// TIME_CENTROID column
	TIME_CENTROID_col = new ROScalarColumn<double> (read_table, "TIME_CENTROID");
	td.addColumn (ScalarColumnDesc<double>("TIME_CENTROID"));

	// UVW column
	UVW_col= new ROArrayColumn<Double> (read_table, "UVW");
	IPosition UVW_pos = UVW_col->shape(0);
	td.addColumn (ArrayColumnDesc<Double>("UVW", UVW_pos, ColumnDesc::Direct));

	// FLAG col->mn
	FLAG_col= new ROArrayColumn<bool> (read_table, "FLAG");
	IPosition FLAG_pos = FLAG_col->shape(0);
	td.addColumn (ArrayColumnDesc<bool>("FLAG", FLAG_pos, ColumnDesc::Direct));

	// WEIGHT col->mn
	WEIGHT_col= new ROArrayColumn<float> (read_table, "WEIGHT");
	IPosition WEIGHT_pos = WEIGHT_col->shape(0);
	td.addColumn (ArrayColumnDesc<float>("WEIGHT", WEIGHT_pos, ColumnDesc::Direct));

	// SIGMA col->mn
	SIGMA_col= new ROArrayColumn<float> (read_table, "SIGMA");
	IPosition SIGMA_pos = SIGMA_col->shape(0);
	td.addColumn (ArrayColumnDesc<float>("SIGMA", SIGMA_pos, ColumnDesc::Direct));

	// DATA col->mn
	DATA_col= new ROArrayColumn<Complex> (read_table, "DATA");
	IPosition DATA_pos = DATA_col->shape(0);
	td.addColumn (ArrayColumnDesc<Complex>("DATA", DATA_pos, ColumnDesc::Direct));

	// WEIGHT_SPECTRUM col->mn
	WEIGHT_SPECTRUM_col= new ROArrayColumn<float> (read_table, "WEIGHT_SPECTRUM");
	IPosition WEIGHT_SPECTRUM_pos = WEIGHT_SPECTRUM_col->shape(0);
	td.addColumn (ArrayColumnDesc<float>("WEIGHT_SPECTRUM", WEIGHT_SPECTRUM_pos, ColumnDesc::Direct));

	// CORRECTED_DATA col->mn
	CORRECTED_DATA_col= new ROArrayColumn<Complex> (read_table, "CORRECTED_DATA");
	IPosition CORRECTED_DATA_pos = CORRECTED_DATA_col->shape(0);
	td.addColumn (ArrayColumnDesc<Complex>("CORRECTED_DATA", CORRECTED_DATA_pos, ColumnDesc::Direct));
	
	// ####### column init for read & write
	if(mpiRank>0){
		stringstream filename;
		filename << "/tmp/v" << mpiRank << ".casa";
		file_output = filename.str();
	}

	SetupNewTable newtab(file_output, td, Table::New);
	newtab.bindAll(stman);

	if (mpiSize == 1 && Rows == 0){
		cout << "Single process mode. Duplicating columns ..." << endl;
		write_table = new Table(newtab, TotalRows);
		write_columns();
	}
	if (mpiSize > 1 && Rows == 0){
		cout << "Multiple process mode. Duplicating all rows ..." << endl;
		write_table = new Table(newtab, TotalRows);
		write_rows();
	}

	if (mpiSize == 1 && Rows > 0){
		cout << "Single process mode. Duplicating " << Rows << " rows ..." << endl;
		write_table = new Table(newtab, Rows);
		write_rows();
	}

	if (mpiSize > 1 && Rows > 0){
		cout << "Multiple process mode. Duplicating " << Rows << " rows ..." << endl;
		if (Rows < mpiSize){
			cout << "Number of rows should be larger than MPI size, otherwise MPI-IO will never return" << endl;
			exit(-1);
		}
		write_table = new Table(newtab, Rows);
		write_rows();
	}

	delete FLAG_ROW_col;
	delete ANTENNA1_col;
	delete ANTENNA2_col;
	delete ARRAY_ID_col;
	delete DATA_DESC_ID_col;
	delete FEED1_col;
	delete FEED2_col;
	delete FIELD_ID_col;
	delete PROCESSOR_ID_col;
	delete SCAN_NUMBER_col;
	delete STATE_ID_col;
	delete INTERVAL_col;
	delete EXPOSURE_col;
	delete TIME_col;
	delete TIME_CENTROID_col;
	delete UVW_col;
	delete FLAG_col;
	delete WEIGHT_col;
	delete SIGMA_col;
	delete DATA_col;
	delete WEIGHT_SPECTRUM_col;
	delete CORRECTED_DATA_col;
	delete write_table;

	tictak_add((char*)"end", 0);

	if(mpiRank == 0){
		float Seconds = tictak_total(0);
		cout << "MpiSize," << mpiSize;
		cout << ",Seconds," << Seconds;
	
	}
	return 0;
}


