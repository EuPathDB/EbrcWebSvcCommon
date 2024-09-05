package org.eupathdb.websvccommon.wsfplugin;

import org.gusdb.fgputil.runtime.GusHome;
import org.gusdb.fgputil.runtime.InstanceManager;
import org.gusdb.wdk.model.Utilities;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.question.Question;
import org.gusdb.wdk.model.record.RecordClass;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;

public class PluginUtilities {

  public static Question getContextQuestion(PluginRequest request) throws PluginModelException {
    String questionFullName = request.getContext().get(Utilities.QUERY_CTX_QUESTION);
    return getWdkModel(request.getProjectId()).getQuestionByFullName(questionFullName)
      .orElseThrow(() -> new PluginModelException("Could not find context question: " + questionFullName));
  }

  public static RecordClass getRecordClass(PluginRequest request) throws PluginModelException {
    return getContextQuestion(request).getRecordClass();
  }

  public static WdkModel getWdkModel(PluginRequest request) {
    return getWdkModel(request.getProjectId());
  }

  public static WdkModel getWdkModel(String projectId) {
    return InstanceManager.getInstance(WdkModel.class, GusHome.getGusHome(), projectId);
  }
}
