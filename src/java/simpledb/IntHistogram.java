package simpledb;

import java.util.Arrays;

/** 
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    
    private int nBuckets;
    private int nBucketSize;
    private int min, max;
    private int[] hist, sum;
    private boolean isSumComputed;
    private int nTups;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	this.nBuckets = buckets;
        this.min = min;
        this.max = max;
        this.nBucketSize = (int) Math.ceil( (double) (1.0*max-min+1) / buckets );
        if(this.nBucketSize == 0) this.nBucketSize = 1;
        this.hist = new int[this.nBuckets];
        this.sum  = new int[this.nBuckets + 1];
        this.nTups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int offset = (v == max) 
                ? ( nBuckets - 1 ) 
                : ( (v - min) / nBucketSize );
        hist[offset]++;
        nTups++;
    }
    
    /**
     * Returns Sum( hist[i..j] )
     * @param i
     * @param j
     * @return The sum hist[i..j]
     */
    private int getSum(int i, int j){
        if(isSumComputed)
            return sum[j+1] - sum[i];
        for(int _i = 1; _i <= nBuckets; _i++)
            sum[_i] = hist[_i-1] + sum[_i-1];
        isSumComputed = true;
        return sum[j+1] - sum[i];
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        double selectivity = 0;
        int offset = (v == max) 
                ? ( nBuckets - 1 ) 
                : ( (v - min) / nBucketSize );
        if(v < min || v > max) offset = -1;
        switch(op){
            case EQUALS:
                if(offset != -1)
                    selectivity = ((double) hist[offset] / nBucketSize) / nTups;
                break;
            case NOT_EQUALS:
                if(offset != -1)
                    selectivity = 1 - ((double) hist[offset] / nBucketSize) / nTups;
                else 
                    selectivity = 1;
                break;
            case GREATER_THAN:
            case GREATER_THAN_OR_EQ:
                if(offset != -1){
                    int b_right = (offset + 1) * nBucketSize + min;
                    double b_f = (double) hist[offset] / nTups;
                    double b_part = (double) (b_right - v) / nBucketSize;
                    
                    if(op.equals(Predicate.Op.GREATER_THAN_OR_EQ))
                        selectivity = ((double) hist[offset] / nBucketSize) / nTups;
                    else
                        selectivity = b_f * b_part;
                    selectivity += (double) getSum(offset + 1, nBuckets - 1) / nTups;
                }else
                    selectivity = (v < min) ? 1.0 : 0.0;
                break;
            case LESS_THAN:
            case LESS_THAN_OR_EQ:
                if(offset != -1){
                    int b_left = offset * nBucketSize + min;
                    double b_f = (double) hist[offset] / nTups;
                    double b_part = (double) (v - b_left) / nBucketSize;
                    if(op.equals(Predicate.Op.LESS_THAN_OR_EQ))
                        selectivity = ((double) hist[offset] / nBucketSize) / nTups;
                    else
                        selectivity = b_f * b_part;
                    selectivity += (double) getSum(0, offset - 1) / nTups;
                }else 
                    selectivity = (v > max) ? 1.0 : 0.0;
                break;
        }
        return selectivity;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * 
     */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
         StringBuilder sb = new StringBuilder();
         sb.append("min : ");
         sb.append(min);
         sb.append("\tmax : ");
         sb.append(max);
         sb.append("\nbuckets : ");
         sb.append(nBuckets);
         sb.append("\tbucketSize : ");
         sb.append(nBucketSize);
         sb.append("\nhist : ");
         sb.append(Arrays.toString(hist));
         sb.append("\n");
         return sb.toString();
    }
}
