package hivemall.matrix;

import javax.annotation.Nonnull;

public final class CSRMatrix implements Matrix {

    @Nonnull
    private final int[] columnIndices;
    @Nonnull
    private final double[] values;
    @Nonnull
    private final int[] rowPointers;
    
    private final int meanNumColumns;

    public CSRMatrix(@Nonnull int[] columnIndices, @Nonnull double[] values,
            @Nonnull int[] rowPointers, int meanNumColumns) {
        this.columnIndices = columnIndices;
        this.values = values;
        this.rowPointers = rowPointers;
        this.meanNumColumns = meanNumColumns;
    }

    @Override
    public int meanNumColumns() {
        return meanNumColumns;
    }

    @Override
    public int numRows() {
        return rowPointers.length;
    }

    @Override
    public int numColumns(int row) {
        // TODO
        return 0;
    }

    @Override
    public double get(int row, int column, double defaultValue) {
        // TODO
        return 0;
    }

}
