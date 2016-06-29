/*
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2016
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
*/

/**
To run this:

First download the gephi toolkit from:
https://gephi.org/toolkit/

cd /Users/Chen/proj/dfms/dfms/deploy/utils
javac -cp /Users/Chen/proj/gephi-toolkit/gephi-toolkit-0.9.1-all.jar -d /tmp/classes export_graph.java
java -classpath /tmp/classes:/Users/Chen/proj/gephi-toolkit/gephi-toolkit-0.9.1-all.jar dfms.deploy.utils.export_graph /Users/Chen/proj/dfms/dfms/deploy/utils/data/test/tianhe_graph.gexf /tmp/my.png

*/

package dfms.deploy.utils;

import java.io.IOException;
import java.io.File;

import org.gephi.io.exporter.api.ExportController;
import org.gephi.io.importer.api.Container;
import org.gephi.io.importer.api.EdgeDirectionDefault;
import org.gephi.io.importer.api.ImportController;
import org.gephi.io.processor.plugin.DefaultProcessor;
import org.gephi.preview.api.PreviewController;
import org.gephi.preview.api.PreviewModel;
import org.gephi.preview.api.PreviewProperty;
import org.gephi.preview.types.EdgeColor;
import org.gephi.graph.api.GraphModel;
import org.gephi.graph.api.GraphController;
import org.gephi.graph.api.DirectedGraph;
import org.gephi.project.api.ProjectController;
import org.gephi.project.api.Workspace;

import org.openide.util.Lookup;

public class export_graph {
  /**
  Two parameters: input gexf file and ouput png/pdf file
  */
  public static void main(String[] args) {
      //System.out.println("arg leng = " + args.length);
      if (args.length != 2) {
        System.out.println("Usage: java dfms.deploy.utils.export_graph <input_gexf> <output_png>");
        System.exit(1);
      }
      String input_gexf = args[0];
      String output_png = args[1];

      //Init a project - and therefore a workspace
      ProjectController pc = Lookup.getDefault().lookup(ProjectController.class);
      pc.newProject();
      Workspace workspace = pc.getCurrentWorkspace();
      GraphModel graphModel = Lookup.getDefault().lookup(GraphController.class).getGraphModel();
      PreviewModel previewModel = Lookup.getDefault().lookup(PreviewController.class).getModel();
      ImportController importController = Lookup.getDefault().lookup(ImportController.class);
      //Import file
      Container container;
      try {
          File file = new File(input_gexf);
          container = importController.importFile(file);
          container.getLoader().setEdgeDefault(EdgeDirectionDefault.DIRECTED);   //Force DIRECTED
      } catch (Exception ex) {
          ex.printStackTrace();
          return;
      }

      //Append imported data to GraphAPI
      importController.process(container, new DefaultProcessor(), workspace);

      //See if graph is well imported
      DirectedGraph graph = graphModel.getDirectedGraph();
      System.out.println("Nodes: " + graph.getNodeCount());
      System.out.println("Edges: " + graph.getEdgeCount());

      //Preview
      previewModel.getProperties().putValue(PreviewProperty.SHOW_NODE_LABELS, Boolean.FALSE);
      previewModel.getProperties().putValue(PreviewProperty.SHOW_EDGE_LABELS, Boolean.FALSE);
      previewModel.getProperties().putValue(PreviewProperty.EDGE_RESCALE_WEIGHT, Boolean.TRUE);
      previewModel.getProperties().putValue(PreviewProperty.EDGE_COLOR, new EdgeColor(EdgeColor.Mode.ORIGINAL));
      previewModel.getProperties().putValue(PreviewProperty.EDGE_THICKNESS, new Float(3.0f));
      previewModel.getProperties().putValue(PreviewProperty.NODE_OPACITY, new Float(50.0f));

      //Export
      ExportController ec = Lookup.getDefault().lookup(ExportController.class);
      try {
          ec.exportFile(new File(output_png));
      } catch (IOException ex) {
          ex.printStackTrace();
          return;
      }


    }

}
