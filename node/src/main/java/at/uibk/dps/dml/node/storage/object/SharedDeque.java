package at.uibk.dps.dml.node.storage.object;

import java.io.Serializable;
import java.util.ArrayDeque;

/**
 * The {@link SharedDeque} holds a {@link ArrayDeque}.
 */
public class SharedDeque<E> extends ArrayDeque<E> implements Serializable {
}
