package org.eupathdb.websvccommon.wsfplugin.blast;

import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Logger;
import org.gusdb.fgputil.Tuples.TwoTuple;
import org.gusdb.wsf.plugin.PluginUserException;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Encapsulates the processing and conversion of multi-blast service params from
 * their WDK question form representation into the JSON accepted by the
 * multi-blast service.
 * <p>
 * NOTE: This is a transcription of the logic contained in:
 * <p>
 *     web-multi-blast/blob/main/src/lib/utils/params-query.ts
 * <p>
 * The two must be kept in sync so unexpected results are not shown in the
 * multi-blast UI and so users get the same result when they export to WDK.
 *
 * @author rdoherty
 * @author epharper
 * @author jtlong
 */
public class MultiBlastServiceParams {

  private static final Logger LOG = Logger.getLogger(MultiBlastServiceParams.class);

  public static final String BLAST_DATABASE_ORGANISM_PARAM_NAME = "BlastDatabaseOrganism";
  public static final String BLAST_DATABASE_TYPE_PARAM_NAME = "MultiBlastDatabaseType";
  public static final String BLAST_QUERY_SEQUENCE_PARAM_NAME = "BlastQuerySequence";
  public static final String BLAST_ALGORITHM_PARAM_NAME = "BlastAlgorithm";

  // General config for all BLAST applications
  public static final String EXPECTATION_VALUE_PARAM_NAME = "ExpectationValue";
  public static final String NUM_QUERY_RESULTS_PARAM_NAME = "NumQueryResults";
  public static final String MAX_MATCHES_QUERY_RANGE_PARAM_NAME = "MaxMatchesQueryRange";

  // General config specific to each BLAST application
  public static final String WORD_SIZE_PARAM_NAME = "WordSize";
  public static final String SCORING_MATRIX_PARAM_NAME = "ScoringMatrix";
  public static final String COMP_ADJUST_PARAM_NAME = "CompAdjust";

  // Filter and masking config
  public static final String FILTER_LOW_COMPLEX_PARAM_NAME = "FilterLowComplex";
  public static final String SOFT_MASK_PARAM_NAME = "SoftMask";
  public static final String LOWER_CASE_MASK_PARAM_NAME = "LowerCaseMask";

  // Scoring config
  public static final String GAP_COSTS_PARAM_NAME = "GapCosts";
  public static final String MATCH_MISMATCH_SCORE = "MatchMismatchScore";

  public static String[] getAllParamNames() {
    return new String[] {
      BLAST_DATABASE_TYPE_PARAM_NAME,
      BLAST_ALGORITHM_PARAM_NAME,
      BLAST_DATABASE_ORGANISM_PARAM_NAME,
      BLAST_QUERY_SEQUENCE_PARAM_NAME,
      EXPECTATION_VALUE_PARAM_NAME,
      NUM_QUERY_RESULTS_PARAM_NAME,
      MAX_MATCHES_QUERY_RANGE_PARAM_NAME,
      WORD_SIZE_PARAM_NAME,
      SCORING_MATRIX_PARAM_NAME,
      MATCH_MISMATCH_SCORE,
      GAP_COSTS_PARAM_NAME,
      COMP_ADJUST_PARAM_NAME,
      FILTER_LOW_COMPLEX_PARAM_NAME,
      SOFT_MASK_PARAM_NAME,
      LOWER_CASE_MASK_PARAM_NAME
    };
  }

  public static JSONObject buildBlastCreateJobBody(String projectID, Map<String, String> params)
  throws PluginUserException {
    return new JSONObject()
      .put("jobConfig", paramValuesToBlastJobConfig(projectID, params))
      .put("blastConfig", paramValuesToBlastQueryConfig(params));
  }

  private static JSONObject paramValuesToBlastJobConfig(String projectID, Map<String, String> params) {
    return new JSONObject()
      .put("site", projectID)
      .put("targets", buildNewJobRequestTargetJson(params))
      .put("query", getNormalizedParamValue(params, BLAST_QUERY_SEQUENCE_PARAM_NAME))
      .put("addToUserCollection", false);
  }

  private static JSONObject paramValuesToBlastQueryConfig(Map<String, String> params)  throws PluginUserException {
    final var tool = getNormalizedParamValue(params, BLAST_ALGORITHM_PARAM_NAME);

    final var config = new JSONObject()
      .put("eValue", getNormalizedParamValue(params, EXPECTATION_VALUE_PARAM_NAME))
      .put("softMasking", getBooleanParamValue(params, SOFT_MASK_PARAM_NAME))
      .put("lowercaseMasking", getBooleanParamValue(params, LOWER_CASE_MASK_PARAM_NAME))
      .put("maxTargetSequences", getIntParamValue(params, NUM_QUERY_RESULTS_PARAM_NAME))
      .put("maxHSPs", getIntParamValue(params, MAX_MATCHES_QUERY_RANGE_PARAM_NAME));

    switch (tool) {
      case "blastn":
        return paramsToBlastNConfig(config, params);
      case "blastp":
        return paramsToBlastPConfig(config, params);
      case "blastx":
        return paramsToBlastXConfig(config, params);
      case "tblastn":
        return paramsToTBlastNConfig(config, params);
      case "tblastx":
        return paramsToTBlastXConfig(config, params);
      case "deltablast":
        return paramsToDeltaBlastConfig(config, params);
      case "psiblast":
        return paramsToPSIBlastConfig(config, params);
      case "rpsblast":
        return paramsToRPSBlastConfig(config, params);
      case "rpstblastn":
        return paramsToRPSTBlastNConfig(config, params);
      default:
        throw new PluginUserException("Unknown blast tool: " + tool);
    }
  }

  // // //
  //
  //   MultiBlast Tool-Specific Config Builders
  //
  // // //

  private static JSONObject paramsToBlastNConfig(JSONObject config, Map<String, String> params) {
    var gapCosts = paramValueToIntPair(getNormalizedParamValue(params, GAP_COSTS_PARAM_NAME));
    var mismatch = paramValueToIntPair(getNormalizedParamValue(params, MATCH_MISMATCH_SCORE));

    return config.put("tool", "blastn")
      .put("task", "blastn")
      .put("gapOpen", gapCosts.getFirst())
      .put("gapExtend", gapCosts.getSecond())
      .put("reward", mismatch.getFirst())
      .put("penalty", mismatch.getSecond())
      .put("wordSize", getIntParamValue(params, WORD_SIZE_PARAM_NAME))
      .put("dust", parseDust(params));
  }

  private static JSONObject paramsToBlastPConfig(JSONObject config, Map<String, String> params) {
    var gapCosts = paramValueToIntPair(getNormalizedParamValue(params, GAP_COSTS_PARAM_NAME));

    return config.put("tool", "blastp")
      .put("task", "blastp")
      .put("gapOpen", gapCosts.getFirst())
      .put("gapExtend", gapCosts.getSecond())
      .put("wordSize", getIntParamValue(params, WORD_SIZE_PARAM_NAME))
      .put("matrix", getNormalizedParamValue(params, SCORING_MATRIX_PARAM_NAME))
      .put("compBasedStats", getNormalizedParamValue(params, COMP_ADJUST_PARAM_NAME))
      .put("seg", parseSeg(params));
  }

  private static JSONObject paramsToBlastXConfig(JSONObject config, Map<String, String> params) {
    var gapCosts = paramValueToIntPair(getNormalizedParamValue(params, GAP_COSTS_PARAM_NAME));

    return config.put("tool", "blastx")
      .put("task", "blastx")
      .put("gapOpen", gapCosts.getFirst())
      .put("gapExtend", gapCosts.getSecond())
      .put("wordSize", getIntParamValue(params, WORD_SIZE_PARAM_NAME))
      .put("matrix", getNormalizedParamValue(params, SCORING_MATRIX_PARAM_NAME))
      .put("compBasedStats", getNormalizedParamValue(params, COMP_ADJUST_PARAM_NAME))
      .put("seg", parseSeg(params));
  }

  private static JSONObject paramsToTBlastNConfig(JSONObject config, Map<String, String> params) {
    var gapCosts = paramValueToIntPair(getNormalizedParamValue(params, GAP_COSTS_PARAM_NAME));

    return config.put("tool", "tblastn")
      .put("task", "tblastn")
      .put("gapOpen", gapCosts.getFirst())
      .put("gapExtend", gapCosts.getSecond())
      .put("wordSize", getIntParamValue(params, WORD_SIZE_PARAM_NAME))
      .put("matrix", getNormalizedParamValue(params, SCORING_MATRIX_PARAM_NAME))
      .put("compBasedStats", getNormalizedParamValue(params, COMP_ADJUST_PARAM_NAME))
      .put("seg", parseSeg(params));
  }

  private static JSONObject paramsToTBlastXConfig(JSONObject config, Map<String, String> params) {
    return config.put("tool", "tblastx")
      .put("wordSize", getIntParamValue(params, WORD_SIZE_PARAM_NAME))
      .put("matrix", getNormalizedParamValue(params, SCORING_MATRIX_PARAM_NAME))
      .put("seg", parseSeg(params));
  }

  private static JSONObject paramsToDeltaBlastConfig(JSONObject config, Map<String, String> params) {
    var gapCosts = paramValueToIntPair(getNormalizedParamValue(params, GAP_COSTS_PARAM_NAME));

    return config.put("tool", "deltablast")
      .put("gapOpen", gapCosts.getFirst())
      .put("gapExtend", gapCosts.getSecond())
      .put("wordSize", getIntParamValue(params, WORD_SIZE_PARAM_NAME))
      .put("matrix", getNormalizedParamValue(params, SCORING_MATRIX_PARAM_NAME))
      .put("compBasedStats", getNormalizedParamValue(params, COMP_ADJUST_PARAM_NAME))
      .put("seg", parseSeg(params));
  }

  private static JSONObject paramsToPSIBlastConfig(JSONObject config, Map<String, String> params) {
    var gapCosts = paramValueToIntPair(getNormalizedParamValue(params, GAP_COSTS_PARAM_NAME));

    return config.put("tool", "psiblast")
      .put("gapOpen", gapCosts.getFirst())
      .put("gapExtend", gapCosts.getSecond())
      .put("wordSize", getIntParamValue(params, WORD_SIZE_PARAM_NAME))
      .put("matrix", getNormalizedParamValue(params, SCORING_MATRIX_PARAM_NAME))
      .put("compBasedStats", getNormalizedParamValue(params, COMP_ADJUST_PARAM_NAME))
      .put("seg", parseSeg(params));
  }

  private static JSONObject paramsToRPSBlastConfig(JSONObject config, Map<String, String> params) {
    return config.put("tool", "rpsblast")
      .put("compBasedStats", getNormalizedParamValue(params, COMP_ADJUST_PARAM_NAME))
      .put("seg", parseSeg(params));
  }

  private static JSONObject paramsToRPSTBlastNConfig(JSONObject config, Map<String, String> params) {
    return config.put("tool", "rpstblastn")
      .put("compBasedStats", getNormalizedParamValue(params, COMP_ADJUST_PARAM_NAME))
      .put("seg", parseSeg(params));
  }

  // // //
  //
  //   Helper Methods
  //
  // // //

  private static String getNormalizedParamValue(Map<String, String> params, String paramName) {
    return params.get(paramName).replaceAll("^'|'$", "");
  }

  private static boolean getBooleanParamValue(Map<String, String> params, String paramName) {
    return paramValueToBoolean(getNormalizedParamValue(params, paramName));
  }

  private static int getIntParamValue(Map<String, String> params, String paramName) {
    return paramValueToInt(getNormalizedParamValue(params, paramName));
  }

  private static JSONObject parseDust(Map<String, String> params) {
    return new JSONObject()
      .put("enabled", !getNormalizedParamValue(params, FILTER_LOW_COMPLEX_PARAM_NAME).startsWith("no"));
  }

  private static JSONObject parseSeg(Map<String, String> params) {
    return new JSONObject()
      .put("enabled", !getNormalizedParamValue(params, FILTER_LOW_COMPLEX_PARAM_NAME).startsWith("no"));
  }


  /**
   * Converts the internal values of the WDK multiblast query params into
   * a JSON array passed to the multi-blast service which specifies the
   * databases the job should target
   *
   * @param params internal values of params
   * @return json array to be passed as "targets" to multi-blast service
   */
  private static JSONArray buildNewJobRequestTargetJson(Map<String, String> params) {
    var organismsStr = params.get(BLAST_DATABASE_ORGANISM_PARAM_NAME);
    var wdkTargetType = params.get(BLAST_DATABASE_TYPE_PARAM_NAME);

    var organisms = organismsStr.split(",");

    // FIXME This is a carryover of some hardcoding from
    // ApiCommonWebService's EuPathBlastCommandFormatter.
    // We should explore more permanent solutions.
    var blastTargetType = wdkTargetType.equals("PopSet")
      ? "Isolates"
      : wdkTargetType;

    var targets = Arrays.stream(organisms)
      .filter(organism -> !(organism.length() <= 3))
      .map(leafOrganism ->
        new JSONObject()
          .put("targetDisplayName", leafOrganism)
          .put("targetFile", leafOrganism + blastTargetType)
      )
      .toArray();

    return new JSONArray(targets);
  }


  private static boolean paramValueToBoolean(String paramValue) {
    return paramValue.equals("true");
  }

  private static int paramValueToInt(String paramValue) {
    return Integer.parseInt(paramValue);
  }

  private static TwoTuple<Integer, Integer> paramValueToIntPair(String paramValue) {
    var pairStrValues = paramValue.split(",", 2);

    return new TwoTuple<>(
      Integer.parseInt(pairStrValues[0]),
      Integer.parseInt(pairStrValues[1])
    );
  }
}
