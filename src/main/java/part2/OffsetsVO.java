package part2;

import common.Constants;

public class OffsetsVO {
	private long lineNumberOffset = 0;
	private long rankOffset = 0;
	private long denseRankOffset = 0;

	public OffsetsVO(long lineNumberOffset, long rankOffset, long denseRankOffset) {
		this.lineNumberOffset = lineNumberOffset;
		this.rankOffset = rankOffset;
		this.denseRankOffset = denseRankOffset;
	}

	public long getLineNumberOffset() {
		return lineNumberOffset;
	}

	public long getRankOffset() {
		return rankOffset;
	}

	public long getDenseRankOffset() {
		return denseRankOffset;
	}
	
	public void setLineNumberOffset(long lineNumberOffset) {
		this.lineNumberOffset = lineNumberOffset;
	}

	public void setRankOffset(long rankOffset) {
		this.rankOffset = rankOffset;
	}

	public void setDenseRankOffset(long denseRankOffset) {
		this.denseRankOffset = denseRankOffset;
	}

	@Override
	public String toString() {
		return String.valueOf(lineNumberOffset) + Constants.SPACE + String.valueOf(rankOffset) + Constants.SPACE
				+ String.valueOf(denseRankOffset);
	}
}
