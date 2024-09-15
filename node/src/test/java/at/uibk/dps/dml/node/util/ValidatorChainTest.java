package at.uibk.dps.dml.node.util;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class ValidatorChainTest {

    @Test
    void testGetErrorReturnsEmptyOptionalIfValidationSucceeds() {
        ValidatorChain<Throwable> validatorChain = new ValidatorChain<>();
        validatorChain
                .validate(() -> null)
                .validate(() -> null);

        Optional<Throwable> error = validatorChain.getError();

        assertTrue(error.isEmpty());
    }

    @Test
    void testGetErrorReturnsFirstExceptionIfValidationFails() {
        ValidatorChain<Throwable> validatorChain = new ValidatorChain<>();
        validatorChain
                .validate(() -> null)
                .validate(NullPointerException::new)
                .validate(IllegalArgumentException::new);

        Optional<Throwable> error = validatorChain.getError();

        assertTrue(error.isPresent());
        assertInstanceOf(NullPointerException.class, error.get());
    }
}
