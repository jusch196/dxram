package de.hhu.bsinfo.dxgraph.algo.bfs.front;

/**
 * Implementation of a frontier list. Extended version
 * of a normal BitVector keeping the first value
 * within the vector cached to speed up element search.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.03.16
 *
 */
public class BitVectorWithStartPos implements FrontierList
{
	private long[] m_vector = null;		
	
	private long m_itPos = 0;
	private long m_count = 0;
	private long m_firstValuePos = -1;
	
	public BitVectorWithStartPos(final long p_vertexCount)
	{
		m_vector = new long[(int) ((p_vertexCount / 64L) + 1L)];
	}
	
	@Override
	public void pushBack(final long p_index)
	{
		long tmp = (1L << (p_index % 64L));
		int idx = (int) (p_index / 64L);
		if ((m_vector[idx] & tmp) == 0)
		{				
			m_count++;
			m_vector[idx] |= tmp;
			if (m_firstValuePos > p_index) {
				m_firstValuePos = p_index;
			}
		}
	}
	
	@Override
	public long size()
	{
		return m_count;
	}
	
	@Override
	public boolean isEmpty()
	{
		return m_count == 0;
	}
	
	@Override
	public void reset()
	{
		m_itPos = 0;
		m_count = 0;
		m_firstValuePos = m_vector.length * 64L;
		for (int i = 0; i < m_vector.length; i++) {
			m_vector[i] = 0;
		}
	}
	
	@Override
	public long popFront()
	{		
		while (m_count > 0)
		{
			// speed things up for first value, jump
			if (m_firstValuePos > m_itPos) {
				m_itPos = m_firstValuePos;
			}
			
			if ((m_vector[(int) (m_itPos / 64L)] & (1L << m_itPos % 64L)) != 0)
			{
				long tmp = m_itPos;
				m_itPos++;	
				m_count--;
				return tmp;
			}

			m_itPos++;
		}
		
		return -1;
	}
	
	@Override
	public String toString()
	{
		return "[m_count " + m_count + ", m_itPos " + m_itPos + "]"; 
	}
}
