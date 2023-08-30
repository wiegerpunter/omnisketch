package omniVLDB;

public class DetermineB {

    private final long ram;
    private int depth;
    private int width;
    private int numAttrsToUse;
    private double delta;
    public DetermineB(long ram) {
        this.ram = ram;
    }

    public int determineB(int depth, int width, int p, double delta) {
        this.depth = depth;
        this.width = width;
        this.numAttrsToUse = p;
        this.delta = delta;
        return search(0, 2, 0);
    }
    int highB = Integer.MAX_VALUE;
    public int search(int prevB, int B, int iters) {
        if (B <= 1) {
            return B;
        }
        if (iters > 100){
            if (compRam(prevB) < ram) {
                return prevB;
            } else {
                throw new RuntimeException("Too many iterations");
            }
        }
        iters++;
        double usedM = compRam(B);

        if (ram*0.99 <= usedM && usedM <= ram) {
            return B;
        } else if (usedM < ram*0.99) {
            if (highB == Integer.MAX_VALUE)
                return search(B, B*2, iters);
            else
                return search(B, Math.min(highB, B*2), iters);
        } else {
            highB = B;
            return search(prevB, (int) Math.ceil((double) (B + prevB)/2), iters);
        }
    }

    public double compRam(int B) {
        int smallb = (int) Math.ceil(Math.log(4*Math.pow(B, (double) 5/2)/delta));

        //return (double) depth * width*(B * (smallb) + 32) * numAttrsToUse;
        if (Main.rangeQueries) {
            return Main.dyadicRangeBits /Math.log(2) * depth * width*(B * (smallb + 3 * 32 + 1) + 32) * numAttrsToUse;
        } else {
            return (double) depth * width*(B * (smallb + 3 * 32 + 1) + 32) * numAttrsToUse;
        }
    }


}
