package edu.berkeley.sparrow.daemon.util;

public class MovingAverage extends FixedLengthQueue {

    private float maNumerator;
    private float maValue;

    public MovingAverage(Number[] initialValues) {
        super(initialValues);
        maNumerator = 0.0f;
        maValue = 0.0f;
        initialize();
    }

    public float getValue() {
        return maValue;
    }

    @Override
    public boolean add(Number newest) {
        return this.offer(newest);
    }

    @Override
    public boolean offer(Number newest) {
        maNumerator -= ring[index].floatValue();

        boolean res = super.offer(newest);

        maNumerator += ring[getHeadIndex()].floatValue();
        maValue = maNumerator / (float) ring.length;

        return res;
    }

    private void initialize() {
        for (int i = previousIndex(index), n = 0; n < ring.length; i = previousIndex(i), n++) {
            maNumerator += ring[i].floatValue();
        }
        maValue = maNumerator / (float) ring.length;
    }
}