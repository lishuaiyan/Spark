package net.duxingzhe.luke.spark.java.rdd.util;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * @author op1982
 */
public class SecondSortKey implements Serializable, Ordered<SecondSortKey> {
    private static final long serialVersionUID = -2749925310062789494L;
    private Double first;
    private Double second;

    public SecondSortKey(Double first, Double second) {
        super();
        this.first = first;
        this.second = second;
    }

    public Double getFirst() {
        return first;
    }

    public void setFirst(Double first) {
        this.first = first;
    }

    public Double getSecond() {
        return second;
    }

    public void setSecond(Double second) {
        this.second = second;
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * first.hashCode() + second.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        SecondSortKey other = (SecondSortKey) obj;
        if (first == null) {
            if (other.first != null) {
                return false;
            }
        } else if (!first.equals(other.first)) {
            return false;
        }
        if (!second.equals(other.second)) {
            return false;
        }
        return true;
    }
    @Override
    public boolean $greater(SecondSortKey that) {
        if (this.first.compareTo(that.getFirst()) > 0) {
            return true;
        }
        return this.first.equals(that.getFirst()) && this.second > that.getSecond();
    }
    @Override
    public boolean $greater$eq(SecondSortKey that) {
        if (this.$greater(that)) {
            return true;
        }
        return this.first.equals(that.getFirst()) && this.second.equals(that.getSecond());
    }
    @Override
    public boolean $less(SecondSortKey that) {
        if (this.first.compareTo(that.getFirst()) < 0) {
            return true;
        }
        return this.first.equals(that.getFirst()) && this.second < that.getSecond();
    }
    @Override
    public boolean $less$eq(SecondSortKey that) {
        if (this.$less(that)) {
            return true;
        }
        return this.first.equals(that.getFirst()) && this.second.equals(that.getSecond());
    }
    @Override
    public int compare(SecondSortKey other) {
        if (this.first - other.first != 0) {
            return (int) (this.first - other.first);
        } else {
            if (this.second - other.second > 0) {
                return (int) Math.ceil(this.second - other.second);
            } else if (this.second - other.second < 0) {
                return (int) Math.floor(this.second - other.second);
            } else  {
                return (int) (this.second - other.second);
            }
        }
    }
    @Override
    public int compareTo(SecondSortKey other) {
        if (this.first - other.first != 0) {
            return (int) (this.first - other.first);
        } else {
            if (this.second - other.second > 0) {
                return (int) Math.ceil(this.second - other.second);
            } else if (this.second - other.second < 0) {
                return (int) Math.floor(this.second - other.second);
            } else  {
                return (int) (this.second - other.second);
            }
        }
    }
}
