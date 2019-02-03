package de.hhu.bsinfo.dxram.chunk.data;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxram.chunk.ChunkService;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class ArrayList <T extends AbstractChunk> implements List<T>, RandomAccess, Cloneable, Serializable {

    private ChunkService chunkService;
    private Function<Long, T> factory;

    private static final int DEFAULT_CAPACITY = 1000;

    transient int modCount;
    private long[] elementData;
    private int size;

    public ArrayList(Function <Long, T> factory, ChunkService chunkService) {
        size=0;
        elementData = new long[DEFAULT_CAPACITY];
        this.factory = factory;
        this.chunkService = chunkService;

        //T test = factory.apply(null);
    }

    private class SubList extends ArrayList<T> implements RandomAccess {
        private final ArrayList<T> parent;
        private final int parentOffset;
        private final int offset;
        int size;

        SubList(ArrayList<T> parent, int offset, int fromIndex, int toIndex,Function <Long, T> factory, ChunkService chunkService ) {
            super(factory, chunkService);
            this.parent = parent;
            this.parentOffset = fromIndex;
            this.offset = offset + fromIndex;
            this.size = toIndex - fromIndex;
            this.modCount = ArrayList.this.modCount;
        }

        public T set(int index, T e) {

            rangeCheck(index);
            checkForComodification();

            T chunk = factory.apply(elementData[index]);
            chunkService.put().put(chunk);

            return chunk;
        }

        public T get(int index) {
            rangeCheck(index);
            checkForComodification();

            T chunk = factory.apply(elementData[index]);
            chunkService.get().get(chunk);

            return chunk;
        }

        public int size() {
            checkForComodification();
            return this.size;
        }

        public void add(int index, T e) {

            rangeCheckForAdd(index);
            checkForComodification();

            parent.add(parentOffset + index, e);

            this.modCount = parent.modCount;
            this.size++;

        }

        public T remove(int index) {
            rangeCheck(index);
            checkForComodification();

            T result = parent.remove(parentOffset + index);

            this.modCount = parent.modCount;
            this.size--;

            return result;
        }

        public boolean addAll(Collection<? extends T> c) {
            return addAll(this.size, c);
        }

        public boolean addAll(int index, Collection<? extends T> c) {
            rangeCheckForAdd(index);

            int cSize = c.size();

            if (cSize==0)
                return false;

            checkForComodification();

            parent.addAll(parentOffset + index, c);
            this.modCount = parent.modCount;
            this.size += cSize;
            return true;
        }

        public Iterator<T> iterator() {
            return listIterator();

        }

        public ListIterator<T> listIterator(final int index) {

            checkForComodification();
            rangeCheckForAdd(index);
            final int offset = this.offset;

            return new ListIterator<T>() {

                int cursor = index;
                int lastRet = -1;
                int expectedModCount = ArrayList.this.modCount;

                public boolean hasNext() {
                    return cursor != SubList.this.size;
                }

                @SuppressWarnings("unchecked")
                public T next() {
                    checkForComodification();

                    int i = cursor;

                    if (i >= SubList.this.size)
                        throw new NoSuchElementException();

                    Object[] elementData = new Object[ArrayList.this.size()];

                    for (int j=0;j<ArrayList.this.size();j++){
                        elementData[j] = ArrayList.this.elementData(j);
                    }

                    if (offset + i >= elementData.length)
                        throw new ConcurrentModificationException();

                    cursor = i + 1;
                    return (T) elementData[offset + (lastRet = i)];
                }

                public boolean hasPrevious() {
                    return cursor != 0;
                }

                @SuppressWarnings("unchecked")
                public T previous() {
                    checkForComodification();

                    int i = cursor - 1;

                    if (i < 0)
                        throw new NoSuchElementException();

                    Object[] elementData = new Object[ArrayList.this.size()];
                    for (int j=0; j<ArrayList.this.size(); j++){
                        elementData[j] = ArrayList.this.elementData(j);
                    }

                    if (offset + i >= elementData.length)
                        throw new ConcurrentModificationException();
                    cursor = i;

                    return (T) elementData[offset + (lastRet = i)];
                }

                @SuppressWarnings("unchecked")
                public void forEachRemaining(Consumer<? super T> consumer) {
                    Objects.requireNonNull(consumer);
                    final int size = SubList.this.size;
                    int i = cursor;
                    if (i >= size) {
                        return;
                    }

                    final Object[] elementData = new Object[ArrayList.this.size()];
                    for (int j=0; j<ArrayList.this.size();j++){
                        elementData[j] = ArrayList.this.elementData(j);
                    }


                    if (offset + i >= elementData.length) {
                        throw new ConcurrentModificationException();
                    }
                    while (i != size && modCount == expectedModCount) {
                        consumer.accept((T) elementData[offset + (i++)]);
                    }
                    // update once at end of iteration to reduce heap write traffic
                    lastRet = cursor = i;
                    checkForComodification();
                }

                public int nextIndex() {
                    return cursor;
                }

                public int previousIndex() {
                    return cursor - 1;
                }

                public void remove() {
                    if (lastRet < 0)
                        throw new IllegalStateException();
                    checkForComodification();

                    try {
                        SubList.this.remove(lastRet);
                        cursor = lastRet;
                        lastRet = -1;
                        expectedModCount = ArrayList.this.modCount;
                    } catch (IndexOutOfBoundsException ex) {
                        throw new ConcurrentModificationException();
                    }
                }

                public void set(T e) {
                    if (lastRet < 0)
                        throw new IllegalStateException();
                    checkForComodification();

                    try {
                        ArrayList.this.set(offset + lastRet, e);
                    } catch (IndexOutOfBoundsException ex) {
                        throw new ConcurrentModificationException();
                    }
                }

                public void add(T e) {

                    checkForComodification();

                    try {
                        int i = cursor;
                        SubList.this.add(i, e);
                        cursor = i + 1;
                        lastRet = -1;
                        expectedModCount = ArrayList.this.modCount;
                    } catch (IndexOutOfBoundsException ex) {
                        throw new ConcurrentModificationException();
                    }
                }

                final void checkForComodification() {
                    if (expectedModCount != ArrayList.this.modCount)
                        throw new ConcurrentModificationException();
                }
            };
        }

        public List<T> subList(int fromIndex, int toIndex) {
            subListRangeCheck(fromIndex, toIndex, size);
            return new SubList(this, offset, fromIndex, toIndex, factory, chunkService);
        }

        private void rangeCheck(int index) {
            if (index < 0 || index >= this.size)
                throw new IndexOutOfBoundsException(outOfBoundsMsg(index));
        }

        private void rangeCheckForAdd(int index) {
            if (index < 0 || index > this.size)
                throw new IndexOutOfBoundsException(outOfBoundsMsg(index));
        }

        private String outOfBoundsMsg(int index) {
            return "Index: "+index+", Size: "+this.size;
        }

        private void checkForComodification() {
            if (ArrayList.this.modCount != this.modCount)
                throw new ConcurrentModificationException();
        }

        public Spliterator<T> spliterator() {
            checkForComodification();
            return new ArrayListSpliterator<T>(ArrayList.this, offset,
                    offset + this.size, this.modCount);

        }

    }

    static final class ArrayListSpliterator<T extends AbstractChunk> implements Spliterator<T> {

        private final ArrayList<T> list;
        private int index; // current index, modified on advance/split
        private int fence; // -1 until used; then one past last index
        private int expectedModCount; // initialized when fence set

        ArrayListSpliterator(ArrayList<T> list, int origin, int fence, int expectedModCount) {
            this.list = list; // OK if null unless traversed
            this.index = origin;
            this.fence = fence;
            this.expectedModCount = expectedModCount;
        }

        private int getFence() { // initialize fence to size on first use
            int hi; // (a specialized variant appears in method forEach)
            ArrayList<T> lst;

            if ((hi = fence) < 0) {
                if ((lst = list) == null)
                    hi = fence = 0;
                else {
                    expectedModCount = lst.modCount;
                    hi = fence = lst.size;
                }
            }
            return hi;
        }

        public ArrayListSpliterator<T> trySplit() {
            int hi = getFence(), lo = index, mid = (lo + hi) >>> 1;
            return (lo >= mid) ? null : // divide range in half unless too small
                    new ArrayListSpliterator<T>(list, lo, index = mid,
                            expectedModCount);
        }

        public boolean tryAdvance(Consumer<? super T> action) {

            if (action == null)
                throw new NullPointerException();
            int hi = getFence(), i = index;
            if (i < hi) {
                index = i + 1;
                @SuppressWarnings("unchecked") T e = list.get(i);
                action.accept(e);

                if (list.modCount != expectedModCount)
                    throw new ConcurrentModificationException();

                return true;
            }

            return false;
        }

        public void forEachRemaining(Consumer<? super T> action) {

            int i, hi, mc; // hoist accesses and checks from loop

            ArrayList<T> lst; long[] a;

            if (action == null)
                throw new NullPointerException();

            if ((lst = list) != null && (a = lst.elementData) != null) {
                if ((hi = fence) < 0) {
                    mc = lst.modCount;
                    hi = lst.size;
                }
                else
                    mc = expectedModCount;

                if ((i = index) >= 0 && (index = hi) <= a.length) {
                    for (; i < hi; ++i) {
                        // ???????!!!!!!
                        @SuppressWarnings("unchecked") T e = lst.get(i);
                        action.accept(e);
                    }

                    if (lst.modCount == mc)
                        return;
                }
            }
            throw new ConcurrentModificationException();
        }

        public long estimateSize() {
            return (long) (getFence() - index);
        }

        public int characteristics() {
            return Spliterator.ORDERED | Spliterator.SIZED | Spliterator.SUBSIZED;
        }

    }

    private void extend() {
        int extendedSize = 2*size;
        elementData = Arrays.copyOf(elementData, extendedSize);
    }

    private long elementData(int index) {
        return  elementData[index];
    }

    private static void subListRangeCheck(int fromIndex, int toIndex, int size) {

        if (fromIndex < 0)
            throw new IndexOutOfBoundsException("fromIndex = " + fromIndex);

        if (toIndex > size)
            throw new IndexOutOfBoundsException("toIndex = " + toIndex);

        if (fromIndex > toIndex)
            throw new IllegalArgumentException("fromIndex(" + fromIndex +
                    ") > toIndex(" + toIndex + ")");
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size==0;
    }

    @Override
    public boolean contains(Object o) {
        return indexOf(o) >= 0;
    }

    @NotNull
    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            int pos = 0;

            @Override
            public boolean hasNext() {
                return pos < size;
            }

            @Override
            public T next() {
                if (hasNext())
                    return factory.apply(elementData[pos++]);
                else
                    throw new NoSuchElementException();
            }
        };
    }

    @NotNull
    @Override
    public Object[] toArray() {
        Object[] array = new Object[size];
        for (int i=0; i<size;i++){
            array[i] = elementData[i];
        }
        return array;
    }

    @NotNull
    @Override
    public <T> T[] toArray(@NotNull T[] a) {
        if (a.length < size)
            a = (T[]) Array.newInstance(a.getClass().getComponentType(), size);
        else if (a.length > size)
            a[size] = null;

        System.arraycopy(elementData, 0, a, 0, size);
        return a;
    }

    @Override
    public boolean add(T chunk) {

        if (size== elementData.length)
            extend();
        try {
            int i = size;
            this.add(i, chunk);
            size = i + 1;
        } catch (IndexOutOfBoundsException ex) {
            throw new ConcurrentModificationException();
        }
        return true;
    }

    @Override
    public boolean remove(Object o) {
        de.hhu.bsinfo.dxmem.data.AbstractChunk chunk = (de.hhu.bsinfo.dxmem.data.AbstractChunk) o;
        long id = chunk.getID();

        for (int i=0; i<size; i++){
            if (elementData[i] == id){
                System.arraycopy(elementData, i+1, elementData,i, size-(i+1) );
                size--;
            }
        }
        return true;
    }

    @Override
    public boolean containsAll(@NotNull Collection<?> c) {
        Iterator<T> itr = (Iterator<T>) c.iterator();

        boolean ret = false;

        while (itr.hasNext()){
            if (!contains(itr.next()))
                return false;
        }
        return true;
    }

    @Override
    public boolean addAll(@NotNull Collection<? extends T> c) {
        return addAll(size, c);
    }

    @Override
    public boolean addAll(int index, @NotNull Collection<? extends T> c) {
        if (index<size){

            Iterator<de.hhu.bsinfo.dxmem.data.AbstractChunk> itr = (Iterator<de.hhu.bsinfo.dxmem.data.AbstractChunk>) c.iterator();
            int csize = c.size();
            while (csize + size > elementData.length)
                extend();

            int end = index + csize;
            if (size > 0 && index != size)
                System.arraycopy(elementData, index, elementData, end, size - index);
            size += csize;

            for ( ; index < end; index++) {
                elementData[index] = itr.next().getID();
            }
            return csize > 0;
        }
        else throw new IndexOutOfBoundsException();
    }

    @Override
    public boolean removeAll(@NotNull Collection<?> c) {

        int i;
        int j;
        for (i = 0; i < size; i++){
            if (c.contains(elementData[i]))
                break;
        }
        if (i == size)
            return false;
        for (j = i++; i < size; i++)
            if (! c.contains(elementData[i]))
                elementData[j++] = elementData[i];

        size -= i - j;
        return true;
    }

    @Override
    public boolean retainAll(@NotNull Collection<?> c) {
        int i;
        int j;

        for (i = 0; i < size; i++)
            if (! c.contains(elementData[i]))
                break;

        if (i == size)
            return false;

        for (j = i++; i < size; i++)
            if (c.contains(elementData[i]))
                elementData[j++] = elementData[i];
        size -= i - j;
        return true;
    }

    @Override
    public void clear() {
        size =0;
    }

    @Override
    public T get(int index){
        T chunk = factory.apply(elementData[index]);
        chunkService.get().get(chunk);

        return chunk;
    }

    @Override
    public T set(int index, T element) {
        T chunk = factory.apply(elementData[index]);
        chunkService.put().put(chunk);

        return chunk;
    }

    @Override
    public void add(int index, T element) {
        if (index<size){
            if (size== elementData.length)
                extend();

            size++;
            for (int i=size; i>index;i++){
                elementData[i] = elementData[i-1];
            }
            elementData[index] = element.getID();
        } else throw new IndexOutOfBoundsException();
    }

    @Override
    public T remove(int index) {
        if (index<size){
            for (;index<size-1;index++){
                elementData[index] = elementData[index+1];
            }
        } else throw new IndexOutOfBoundsException();
        return null;
    }

    @Override
    public int indexOf(Object o) {
        if (o == null) {
            for (int i = 0; i < size; i++)
                if (elementData[i] == de.hhu.bsinfo.dxmem.data.ChunkID.INVALID_ID)
                    return i;
        } else {
            for (int i = 0; i < size; i++)
                if (o.equals(elementData[i]))

                    return i;
        }
        return -1;
    }

    @Override
    public int lastIndexOf(Object o) {
        if (o == null) {

            for (int i = size-1; i >= 0; i--)
                if (elementData[i]== de.hhu.bsinfo.dxmem.data.ChunkID.INVALID_ID)
                    return i;
        } else {
            for (int i = size-1; i >= 0; i--)
                if (o.equals(elementData[i]))
                    return i;
        }
        return -1;
    }

    @NotNull
    @Override
    public ListIterator<T> listIterator() {
        return listIterator(0);
    }

    @NotNull
    @Override
    public List<T> subList(int fromIndex, int toIndex) {
        subListRangeCheck(fromIndex, toIndex, size);
        return new SubList(this, 0, fromIndex, toIndex, factory, chunkService);
    }

    @Override
    public Spliterator<T> spliterator() {
        return new ArrayListSpliterator<>(this, 0, -1, 0);
    }
}
