package at.uibk.dps.dml.node.util;

import java.util.Optional;
import java.util.function.Supplier;

public class ValidatorChain<R> {

    protected R error = null;

    public ValidatorChain<R> validate(Supplier<R> validator) {
        if (error == null) {
            error = validator.get();
        }
        return this;
    }

    public Optional<R> getError() {
        return Optional.ofNullable(error);
    }
}
