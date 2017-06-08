//
// Data structures and functions definitions for DALiuGE dynamic libraries
// implementing a DropApp
//
// ICRAR - International Centre for Radio Astronomy Research
// (c) UWA - The University of Western Australia, 2017
// Copyright by UWA (in the framework of the ICRAR)
// All rights reserved
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston,
// MA 02111-1307  USA
//

typedef struct _dlg_input_info {
	char *uid;
	char *oid;
	size_t (*read)(char *buf, size_t n);
} dlg_input_info;

typedef struct _dlg_output_info {
	char *uid;
	char *oid;
	size_t (*write)(const char *buf, size_t n);
} dlg_output_info;

typedef struct _dlg_app_info {
	char *uid;
	char *oid;
	dlg_input_info *inputs;
	unsigned short n_inputs;
	dlg_output_info *outputs;
	unsigned short n_output;
	void *data;
} dlg_app_info;

int init_app_drop(dlg_app_info *drop);

int run(dlg_app_info *drop);