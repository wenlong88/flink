package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.utils.JoinSpec;
import org.apache.flink.table.planner.plan.utils.JoinUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.AbstractStreamingJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.StreamingSemiAntiJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.List;

/**
 * {@link StreamExecNode} for regular Joins.
 *
 * <p>Regular joins are the most generic type of join in which any new records or changes to either
 * side of the join input are visible and are affecting the whole join result.
 */
public class StreamExecJoin extends ExecNodeBase<RowData> implements StreamExecNode<RowData> {

    private final JoinSpec joinSpec;
    private final List<int[]> leftUniqueKeys;
    private final List<int[]> rightUniqueKeys;

    public StreamExecJoin(
            JoinSpec joinSpec,
            List<int[]> leftUniqueKeys,
            List<int[]> rightUniqueKeys,
            ExecEdge leftEdge,
            ExecEdge rightEdge,
            RowType outputType,
            String description) {
        super(Lists.newArrayList(leftEdge, rightEdge), outputType, description);
        this.joinSpec = joinSpec;
        this.leftUniqueKeys = leftUniqueKeys;
        this.rightUniqueKeys = rightUniqueKeys;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final TableConfig tableConfig = planner.getTableConfig();

        final ExecNode<RowData> leftInput = (ExecNode<RowData>) getInputNodes().get(0);
        final ExecNode<RowData> rightInput = (ExecNode<RowData>) getInputNodes().get(1);

        final Transformation<RowData> leftTransform = leftInput.translateToPlan(planner);
        final Transformation<RowData> rightTransform = rightInput.translateToPlan(planner);

        final RowType leftType = (RowType) leftInput.getOutputType();
        final RowType rightType = (RowType) rightInput.getOutputType();
        JoinUtil.validateJoinSpec(joinSpec, leftType, rightType, true);

        final int[] leftJoinKey = joinSpec.getLeftKeys();
        final int[] rightJoinKey = joinSpec.getRightKeys();

        final InternalTypeInfo<RowData> leftTypeInfo = InternalTypeInfo.of(leftType);
        final JoinInputSideSpec leftInputSpec =
                JoinUtil.analyzeJoinInput(leftTypeInfo, leftJoinKey, leftUniqueKeys);

        final InternalTypeInfo<RowData> rightTypeInfo = InternalTypeInfo.of(leftType);
        final JoinInputSideSpec rightInputSpec =
                JoinUtil.analyzeJoinInput(rightTypeInfo, rightJoinKey, rightUniqueKeys);

        RowDataKeySelector leftSelect =
                KeySelectorUtil.getRowDataSelector(leftJoinKey, leftTypeInfo);
        RowDataKeySelector rightSelect =
                KeySelectorUtil.getRowDataSelector(rightJoinKey, rightTypeInfo);

        GeneratedJoinCondition generatedCondition =
                JoinUtil.generateConditionFunction(tableConfig, joinSpec, leftType, rightType);

        long minRetentionTime = tableConfig.getMinIdleStateRetentionTime();

        AbstractStreamingJoinOperator operator;
        FlinkJoinType joinType = joinSpec.getJoinType();
        if (joinType == FlinkJoinType.ANTI || joinType == FlinkJoinType.SEMI) {
            operator =
                    new StreamingSemiAntiJoinOperator(
                            joinType == FlinkJoinType.ANTI,
                            leftTypeInfo,
                            rightTypeInfo,
                            generatedCondition,
                            leftInputSpec,
                            rightInputSpec,
                            joinSpec.getFilterNulls(),
                            minRetentionTime);
        } else {
            boolean leftIsOuter = joinType == FlinkJoinType.LEFT || joinType == FlinkJoinType.FULL;
            boolean rightIsOuter =
                    joinType == FlinkJoinType.RIGHT || joinType == FlinkJoinType.FULL;
            operator =
                    new StreamingJoinOperator(
                            leftTypeInfo,
                            rightTypeInfo,
                            generatedCondition,
                            leftInputSpec,
                            rightInputSpec,
                            leftIsOuter,
                            rightIsOuter,
                            joinSpec.getFilterNulls(),
                            minRetentionTime);
        }

        final RowType returnType = (RowType) getOutputType();
        final TwoInputTransformation<RowData, RowData, RowData> ret =
                new TwoInputTransformation<>(
                        leftTransform,
                        rightTransform,
                        getDesc(),
                        operator,
                        InternalTypeInfo.of(returnType),
                        leftTransform.getParallelism());

        if (inputsContainSingleton()) {
            ret.setParallelism(1);
            ret.setMaxParallelism(1);
        }

        // set KeyType and Selector for state
        ret.setStateKeySelectors(leftSelect, rightSelect);
        ret.setStateKeyType(leftSelect.getProducedType());
        return ret;
    }
}
