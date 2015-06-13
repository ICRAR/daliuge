//    ms_checker.cc: check if ms_converter wrote measurement sets
//    correctly
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

#define CASACORE_VERSION_2

#ifdef CASACORE_VERSION_1
#include <tables/Tables/TableDesc.h>
#include <tables/Tables/SetupNewTab.h>
#include <tables/Tables/ScaColDesc.h>
#include <tables/Tables/ScalarColumn.h>
#include <tables/Tables/ArrColDesc.h>
#include <tables/Tables/ArrayColumn.h>
#include <casa/namespace.h>
#endif

#ifdef CASACORE_VERSION_2
#include <casacore/tables/Tables/TableDesc.h>
#include <casacore/tables/Tables/SetupNewTab.h>
#include <casacore/tables/Tables/ScaColDesc.h>
#include <casacore/tables/Tables/ScalarColumn.h>
#include <casacore/tables/Tables/ArrColDesc.h>
#include <casacore/tables/Tables/ArrayColumn.h>
#include <casacore/casa/namespace.h>
#endif


#include <AdiosStMan.h>

int main (int argc, char **argv){

	string file_tsm="/scratch/jason/1067892840_tsm.ms";
	string file_adios="/scratch/jason/1067892840_adiosA.ms";

	uInt row_start = 0;

	switch(argc){
		case 4:
			row_start = atoi(argv[3]);
		case 3:
			file_adios = argv[2];
			file_tsm = argv[1];
	}


	cout << "file_tsm = " << file_tsm << endl;
	cout << "file_adios = " << file_adios << endl;

	Table tsm_table(file_tsm);
	Table adios_table(file_adios);

	uInt tsm_rows = tsm_table.nrow();
	uInt adios_rows = adios_table.nrow();

	if (tsm_rows != adios_rows){
		cout << "Warning: number of rows does not match!\n" ;
		cout << "ADIOS rows = " << adios_rows << endl;
		cout << "TSM rows = " << tsm_rows << endl;
		cout << "Will only check available" << endl;
	}

//////////////////////////////////////////////////////////////////////////////////////////////
//  TiledShapeStMan

	ROScalarColumn<bool>    FLAG_ROW_col_tsm(tsm_table, "FLAG_ROW");
	ROScalarColumn<int>     ANTENNA1_col_tsm(tsm_table, "ANTENNA1");
	ROScalarColumn<int>     ANTENNA2_col_tsm(tsm_table, "ANTENNA2");
	ROScalarColumn<int>     ARRAY_ID_col_tsm(tsm_table, "ARRAY_ID");
	ROScalarColumn<int>     DATA_DESC_ID_col_tsm(tsm_table, "DATA_DESC_ID");
	ROScalarColumn<int>     FEED1_col_tsm(tsm_table, "FEED1");
	ROScalarColumn<int>     FEED2_col_tsm(tsm_table, "FEED2");
	ROScalarColumn<int>     FIELD_ID_col_tsm(tsm_table, "FIELD_ID");
	ROScalarColumn<int>     PROCESSOR_ID_col_tsm(tsm_table, "PROCESSOR_ID");
	ROScalarColumn<int>     SCAN_NUMBER_col_tsm(tsm_table, "SCAN_NUMBER");
	ROScalarColumn<int>     STATE_ID_col_tsm(tsm_table, "STATE_ID");
	ROScalarColumn<double>  EXPOSURE_col_tsm(tsm_table, "EXPOSURE");
	ROScalarColumn<double>  INTERVAL_col_tsm(tsm_table, "INTERVAL");
	ROScalarColumn<double>  TIME_col_tsm(tsm_table, "TIME");
	ROScalarColumn<double>  TIME_CENTROID_col_tsm(tsm_table, "TIME_CENTROID");

	ROArrayColumn<bool>     FLAG_col_tsm(tsm_table, "FLAG");
	ROArrayColumn<float>    SIGMA_col_tsm(tsm_table, "SIGMA");
	ROArrayColumn<float>    WEIGHT_col_tsm(tsm_table, "WEIGHT");
	ROArrayColumn<Complex>  DATA_col_tsm(tsm_table, "DATA");
	ROArrayColumn<Double>   UVW_col_tsm(tsm_table, "UVW");

//////////////////////////////////////////////////////////////////////////////////////////////
//  AdiosStMan

	ROScalarColumn<bool>    FLAG_ROW_col_adios(adios_table, "FLAG_ROW");
	ROScalarColumn<int>     ANTENNA1_col_adios(adios_table, "ANTENNA1");
	ROScalarColumn<int>     ANTENNA2_col_adios(adios_table, "ANTENNA2");
	ROScalarColumn<int>     ARRAY_ID_col_adios(adios_table, "ARRAY_ID");
	ROScalarColumn<int>     DATA_DESC_ID_col_adios(adios_table, "DATA_DESC_ID");
	ROScalarColumn<int>     FEED1_col_adios(adios_table, "FEED1");
	ROScalarColumn<int>     FEED2_col_adios(adios_table, "FEED2");
	ROScalarColumn<int>     FIELD_ID_col_adios(adios_table, "FIELD_ID");
	ROScalarColumn<int>     PROCESSOR_ID_col_adios(adios_table, "PROCESSOR_ID");
	ROScalarColumn<int>     SCAN_NUMBER_col_adios(adios_table, "SCAN_NUMBER");
	ROScalarColumn<int>     STATE_ID_col_adios(adios_table, "STATE_ID");
	ROScalarColumn<double>  EXPOSURE_col_adios(adios_table, "EXPOSURE");
	ROScalarColumn<double>  INTERVAL_col_adios(adios_table, "INTERVAL");
	ROScalarColumn<double>  TIME_col_adios(adios_table, "TIME");
	ROScalarColumn<double>  TIME_CENTROID_col_adios(adios_table, "TIME_CENTROID");

	ROArrayColumn<bool>     FLAG_col_adios(adios_table, "FLAG");
	ROArrayColumn<float>    SIGMA_col_adios(adios_table, "SIGMA");
	ROArrayColumn<float>    WEIGHT_col_adios(adios_table, "WEIGHT");
	ROArrayColumn<Complex>  DATA_col_adios(adios_table, "DATA");
	ROArrayColumn<double>   UVW_col_adios(adios_table, "UVW");

	///////////////////////////////////////////


	uInt rows = tsm_rows;
	if(tsm_rows > adios_rows) rows = adios_rows;

	for(int i=row_start; i<rows; i++){


	// FLAG_ROW column
		bool FLAG_ROW_adios = FLAG_ROW_col_adios.get(i);
		bool FLAG_ROW_tsm   = FLAG_ROW_col_tsm.get(i);
		if (FLAG_ROW_adios != FLAG_ROW_tsm){
			cout << "row = " << i << ", column = FLAG_ROW" << endl;
			cout << "adios value = " << FLAG_ROW_adios << ", tsm value = " << FLAG_ROW_tsm << endl;
			exit(-1);
		}
	// ANTENNA1 column
		int ANTENNA1_adios = ANTENNA1_col_adios.get(i);
		int ANTENNA1_tsm   = ANTENNA1_col_tsm.get(i);
		if (ANTENNA1_adios != ANTENNA1_tsm){
			cout << "row = " << i << ", column = ANTENNA1" << endl;
			cout << "adios value = " << ANTENNA1_adios << ", tsm value = " << ANTENNA1_tsm << endl;
			exit(-1);
		}
	// ANTENNA2 column
		int ANTENNA2_adios = ANTENNA2_col_adios.get(i);
		int ANTENNA2_tsm   = ANTENNA2_col_tsm.get(i);
		if (ANTENNA2_adios != ANTENNA2_tsm){
			cout << "row = " << i << ", column = ANTENNA2" << endl;
			cout << "adios value = " << ANTENNA2_adios << ", tsm value = " << ANTENNA2_tsm << endl;
			exit(-1);
		}
	// ARRAY_ID column
		int ARRAY_ID_adios = ARRAY_ID_col_adios.get(i);
		int ARRAY_ID_tsm   = ARRAY_ID_col_tsm.get(i);
		if (ARRAY_ID_adios != ARRAY_ID_tsm){
			cout << "row = " << i << ", column = ARRAY_ID" << endl;
			cout << "adios value = " << ARRAY_ID_adios << ", tsm value = " << ARRAY_ID_tsm << endl;
			exit(-1);
		}
	// DATA_DESC_ID column
		int DATA_DESC_ID_adios = DATA_DESC_ID_col_adios.get(i);
		int DATA_DESC_ID_tsm   = DATA_DESC_ID_col_tsm.get(i);
		if (DATA_DESC_ID_adios != DATA_DESC_ID_tsm){
			cout << "row = " << i << ", column = DATA_DESC_ID" << endl;
			cout << "adios value = " << DATA_DESC_ID_adios << ", tsm value = " << DATA_DESC_ID_tsm << endl;
			exit(-1);
		}
	// FEED1 column
		int FEED1_adios = FEED1_col_adios.get(i);
		int FEED1_tsm   = FEED1_col_tsm.get(i);
		if (FEED1_adios != FEED1_tsm){
			cout << "row = " << i << ", column = FEED1" << endl;
			cout << "adios value = " << FEED1_adios << ", tsm value = " << FEED1_tsm << endl;
			exit(-1);
		}
	// FEED2 column
		int FEED2_adios = FEED2_col_adios.get(i);
		int FEED2_tsm   = FEED2_col_tsm.get(i);
		if (FEED2_adios != FEED2_tsm){
			cout << "row = " << i << ", column = FEED2" << endl;
			cout << "adios value = " << FEED2_adios << ", tsm value = " << FEED2_tsm << endl;
			exit(-1);
		}
	// FIELD_ID column
		int FIELD_ID_adios = FIELD_ID_col_adios.get(i);
		int FIELD_ID_tsm   = FIELD_ID_col_tsm.get(i);
		if (FIELD_ID_adios != FIELD_ID_tsm){
			cout << "row = " << i << ", column = FIELD_ID" << endl;
			cout << "adios value = " << FIELD_ID_adios << ", tsm value = " << FIELD_ID_tsm << endl;
			exit(-1);
		}
	// PROCESSOR_ID column
		int PROCESSOR_ID_adios = PROCESSOR_ID_col_adios.get(i);
		int PROCESSOR_ID_tsm   = PROCESSOR_ID_col_tsm.get(i);
		if (PROCESSOR_ID_adios != PROCESSOR_ID_tsm){
			cout << "row = " << i << ", column = PROCESSOR_ID" << endl;
			cout << "adios value = " << PROCESSOR_ID_adios << ", tsm value = " << PROCESSOR_ID_tsm << endl;
			exit(-1);
		}
	// SCAN_NUMBER column
		int SCAN_NUMBER_adios = SCAN_NUMBER_col_adios.get(i);
		int SCAN_NUMBER_tsm   = SCAN_NUMBER_col_tsm.get(i);
		if (SCAN_NUMBER_adios != SCAN_NUMBER_tsm){
			cout << "row = " << i << ", column = SCAN_NUMBER" << endl;
			cout << "adios value = " << SCAN_NUMBER_adios << ", tsm value = " << SCAN_NUMBER_tsm << endl;
			exit(-1);
		}
	// STATE_ID column
		int STATE_ID_adios = STATE_ID_col_adios.get(i);
		int STATE_ID_tsm   = STATE_ID_col_tsm.get(i);
		if (STATE_ID_adios != STATE_ID_tsm){
			cout << "row = " << i << ", column = STATE_ID" << endl;
			cout << "adios value = " << STATE_ID_adios << ", tsm value = " << STATE_ID_tsm << endl;
			exit(-1);
		}
	// EXPOSURE column
		double EXPOSURE_adios = EXPOSURE_col_adios.get(i);
		double EXPOSURE_tsm   = EXPOSURE_col_tsm.get(i);
		if (EXPOSURE_adios != EXPOSURE_tsm){
			cout << "row = " << i << ", column = EXPOSURE" << endl;
			cout << "adios value = " << EXPOSURE_adios << ", tsm value = " << EXPOSURE_tsm << endl;
			exit(-1);
		}
	// INTERVAL column
		double INTERVAL_adios = INTERVAL_col_adios.get(i);
		double INTERVAL_tsm   = INTERVAL_col_tsm.get(i);
		if (INTERVAL_adios != INTERVAL_tsm){
			cout << "row = " << i << ", column = INTERVAL" << endl;
			cout << "adios value = " << INTERVAL_adios << ", tsm value = " << INTERVAL_tsm << endl;
			exit(-1);
		}
	// TIME column
		double TIME_adios = TIME_col_adios.get(i);
		double TIME_tsm   = TIME_col_tsm.get(i);
		if (TIME_adios != TIME_tsm){
			cout << "row = " << i << ", column = TIME" << endl;
			cout << "adios value = " << TIME_adios << ", tsm value = " << TIME_tsm << endl;
			exit(-1);
		}
	// TIME_CENTROID column
		double TIME_CENTROID_adios = TIME_CENTROID_col_adios.get(i);
		double TIME_CENTROID_tsm   = TIME_CENTROID_col_tsm.get(i);
		if (TIME_CENTROID_adios != TIME_CENTROID_tsm){
			cout << "row = " << i << ", column = TIME_CENTROID" << endl;
			cout << "adios value = " << TIME_CENTROID_adios << ", tsm value = " << TIME_CENTROID_tsm << endl;
			exit(-1);
		}


	// FLAG column
		Array<Bool> FLAG_tsm = FLAG_col_tsm.get(i);
		Vector<Bool> FLAG_tsm_rf = FLAG_tsm.reform(IPosition(1, FLAG_tsm.nelements()));
		Array<Bool> FLAG_adios = FLAG_col_adios.get(i);
		Vector<Bool> FLAG_adios_rf = FLAG_adios.reform(IPosition(1, FLAG_adios.nelements()));
		for(int j=0; j<FLAG_tsm.nelements(); j++){
			if(FLAG_tsm_rf[j] != FLAG_adios_rf[j]){
				cout << "row = " << i << ", column = FLAG, element = " << j << endl;
				cout << "adios value = " << FLAG_adios_rf[j] << ", tsm value = " << FLAG_tsm_rf[j] << endl;
				exit(-1);
			}
		}
	// SIGMA column
		Array<float> SIGMA_tsm = SIGMA_col_tsm.get(i);
		Vector<float> SIGMA_tsm_rf = SIGMA_tsm.reform(IPosition(1, SIGMA_tsm.nelements()));
		Array<float> SIGMA_adios = SIGMA_col_adios.get(i);
		Vector<float> SIGMA_adios_rf = SIGMA_adios.reform(IPosition(1, SIGMA_adios.nelements()));
		for(int j=0; j<SIGMA_tsm.nelements(); j++){
			if(SIGMA_tsm_rf[j] != SIGMA_adios_rf[j]){
				cout << "row = " << i << ", column = SIGMA, element = " << j << endl;
				cout << "adios value = " << SIGMA_adios_rf[j] << ", tsm value = " << SIGMA_tsm_rf[j] << endl;
				exit(-1);
			}
		}
	// WEIGHT column
		Array<float> WEIGHT_tsm = WEIGHT_col_tsm.get(i);
		Vector<float> WEIGHT_tsm_rf = WEIGHT_tsm.reform(IPosition(1, WEIGHT_tsm.nelements()));
		Array<float> WEIGHT_adios = WEIGHT_col_adios.get(i);
		Vector<float> WEIGHT_adios_rf = WEIGHT_adios.reform(IPosition(1, WEIGHT_adios.nelements()));
		for(int j=0; j<WEIGHT_tsm.nelements(); j++){
			if(WEIGHT_tsm_rf[j] != WEIGHT_adios_rf[j]){
				cout << "row = " << i << ", column = WEIGHT, element = " << j << endl;
				cout << "adios value = " << WEIGHT_adios_rf[j] << ", tsm value = " << WEIGHT_tsm_rf[j] << endl;
				exit(-1);
			}
		}
	// DATA column
		Array<Complex> DATA_tsm = DATA_col_tsm.get(i);
		Vector<Complex> DATA_tsm_rf = DATA_tsm.reform(IPosition(1, DATA_tsm.nelements()));
		Array<Complex> DATA_adios = DATA_col_adios.get(i);
		Vector<Complex> DATA_adios_rf = DATA_adios.reform(IPosition(1, DATA_adios.nelements()));
		for(int j=0; j<DATA_tsm.nelements(); j++){
			if(DATA_tsm_rf[j] != DATA_adios_rf[j]){
				cout << "row = " << i << ", column = DATA, element = " << j << endl;
				cout << "adios value = " << DATA_adios_rf[j] << ", tsm value = " << DATA_tsm_rf[j] << endl;
				exit(-1);
			}
		}
	// UVW column
		Array<Double> UVW_tsm = UVW_col_tsm.get(i);
		Vector<Double> UVW_tsm_rf = UVW_tsm.reform(IPosition(1, UVW_tsm.nelements()));
		Array<Double> UVW_adios = UVW_col_adios.get(i);
		Vector<Double> UVW_adios_rf = UVW_adios.reform(IPosition(1, UVW_adios.nelements()));
		for(int j=0; j<UVW_tsm.nelements(); j++){
			if(UVW_tsm_rf[j] != UVW_adios_rf[j]){
				cout << "row = " << i << ", column = UVW, element = " << j << endl;
				cout << "adios value = " << UVW_adios_rf[j] << ", tsm value = " << UVW_tsm_rf[j] << endl;
				exit(-1);
			}
		}

		cout << i << " rows checked, " << (float)(i+1)/rows*100 << "% finished" << endl;

	}


	return 0;
}


