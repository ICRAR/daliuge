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

#ifdef __cplusplus
extern "C" {
#endif

/**
 * The different status a data drop can be found at.
 */
typedef enum _drop_status {
	DROP_INITIALIZED = 0,//!< DROP_INITIALIZED
	DROP_WRITING = 1,    //!< DROP_WRITING
	DROP_COMPLETED = 2,  //!< DROP_COMPLETED
	DROP_ERROR = 3,      //!< DROP_ERROR
	DROP_EXPIRED = 4,    //!< DROP_EXPIRED
	DROP_DELETED = 5     //!< DROP_DELETED
} drop_status;

/**
 * The different status an application drop can be found at.
 */
typedef enum _app_status {
	APP_NOT_RUN = 0, //!< APP_NOT_RUN
	APP_RUNNING = 1, //!< APP_RUNNING
	APP_FINISHED = 2,//!< APP_FINISHED
	APP_ERROR = 3    //!< APP_ERROR
} app_status;

/**
 * Information about a batch-style input. It contains the UID and OID of the
 * drop, its finishing status, plus a read method callback that can be used to
 * consume its contents.
 */
typedef struct _dlg_input_info {
	char *uid;
	char *oid;
	char *name;
	drop_status status;
	size_t (*read)(char *buf, size_t n);
} dlg_input_info;

/**
 * Information about a batch-style input. It contains the UID and OID of the
 * drop. Unlike the dlg_input_info structure, there is no need for a read
 * callback since data coming from a streaming input is fed via the data_written
 * function made available by the library author.
 */
typedef struct _dlg_streaming_input_info {
	char *uid;
	char *oid;
	char *name;
} dlg_streaming_input_info;

/**
 * Information about an application's output. It contains the UID and OID of the
 * drop, plus a write method callback that can be used to fill it with contents.
 */
typedef struct _dlg_output_info {
	char *uid;
	char *oid;
	char *name;
	size_t (*write)(const char *buf, size_t n);
} dlg_output_info;

/**
 * The representation of an application instance. Apart from its UID and OID,
 * an application has an array of inputs (with n_inputs elements),
 * streaming_inputs (with n_streaming_inputs) elements, and outputs (with
 * n_outputs elements). If the application works in a streaming mode, then it
 * is also given the running and done callbacks to signal the framwork that it
 * started working, and that it is done with its work, respectively. Finally,
 * a data pointer holds application-specific data.
 */
typedef struct _dlg_app_info {
    char *appname;
	char *uid;
	char *oid;
	int *ranks;
	unsigned int n_ranks;
	dlg_input_info *inputs;
	unsigned int n_inputs;
	dlg_input_info *streaming_inputs;
	unsigned int n_streaming_inputs;
	dlg_output_info *outputs;
	unsigned int n_outputs;
	void (*running)();
	void (*done)(app_status status);
	void *data;
} dlg_app_info;

/**
 * Initializes a new application. Must be provided by both batch- and streaming-
 * oriented applications.
 *
 * @param app The new application instance
 * @param params An array of 2-element string arrays containing initialization
 *               parameters. The last item of the array is a NULL sentinel.
 *               The rest of the items contains a parameter name in the first
 *               position and its corresponding value in the second. Both the
 *               name and the value are NULL terminated.
 * @return Whether the initialization was successful (0) or not (any other value).
 */
int init(dlg_app_info *app, const char ***params);

/**
 * Provides a simple entry point to execute batch-oriented applications.
 *
 * @param app The application instance
 * @return Whether the execution was successful (0) or not (any other value)
 */
int run(dlg_app_info *app);

/**
 * Receives data written into one of the streaming inputs of the application.
 * This method should be implemented only by streaming-oriented applications.
 *
 * @param app The application instance
 * @param uid The UID of the streaming input the newly arrived data belongs to.
 * @param data The newly arrived data
 * @param n The size of the newly arrived data
 */
void data_written(dlg_app_info *app, const char *uid, const char *data, size_t n);

/**
 * Signals an application that no more data is to be expected from one of its
 * streaming inputs.
 * This method should be implemented only by streaming-oriented applications.
 *
 * @param app The application instance
 * @param uid The UID of the streaming input that has finished.
 * @param status The final status in which the streaming input was found.
 */
void drop_completed(dlg_app_info *app, const char *uid, drop_status status);

#ifdef __cplusplus
}
#endif
