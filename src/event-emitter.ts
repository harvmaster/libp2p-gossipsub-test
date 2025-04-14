// TODO: You'll probably want to use WeakRef's here.

export type EventMap = Record<string, unknown>;

type Listener<T> = (detail: T) => void;

interface ListenerEntry<T> {
  listener: Listener<T>;
  wrappedListener: Listener<T>;
  debounceTime?: number;
  once?: boolean;
}

export class EventEmitter<T extends EventMap> {
  private listeners: Map<keyof T, Set<ListenerEntry<T[keyof T]>>> = new Map();

  on<K extends keyof T>(
    type: K,
    listener: Listener<T[K]>,
    debounceMilliseconds?: number,
  ): Listener<T[K]> {
    const wrappedListener =
      debounceMilliseconds && debounceMilliseconds > 0
        ? this.debounce(listener, debounceMilliseconds)
        : listener;

    if (!this.listeners.has(type)) {
      this.listeners.set(type, new Set());
    }

    const listenerEntry: ListenerEntry<T[K]> = {
      listener,
      wrappedListener,
      debounceTime: debounceMilliseconds,
    };

    this.listeners.get(type)?.add(listenerEntry as ListenerEntry<T[keyof T]>);
    return listener;
  }

  once<K extends keyof T>(
    type: K,
    listener: Listener<T[K]>,
    debounceMilliseconds?: number,
  ): Listener<T[K]> {
    const wrappedListener: Listener<T[K]> = (detail: T[K]) => {
      this.off(type, listener);
      listener(detail);
    };

    const debouncedListener =
      debounceMilliseconds && debounceMilliseconds > 0
        ? this.debounce(wrappedListener, debounceMilliseconds)
        : wrappedListener;

    if (!this.listeners.has(type)) {
      this.listeners.set(type, new Set());
    }

    const listenerEntry: ListenerEntry<T[K]> = {
      listener,
      wrappedListener: debouncedListener,
      debounceTime: debounceMilliseconds,
      once: true,
    };

    this.listeners.get(type)?.add(listenerEntry as ListenerEntry<T[keyof T]>);
    return listener;
  }

  off<K extends keyof T>(type: K, listener: Listener<T[K]>): void {
    const listeners = this.listeners.get(type);
    if (!listeners) return;

    const listenerEntry = Array.from(listeners).find(
      (entry) =>
        entry.listener === listener || entry.wrappedListener === listener,
    );

    if (listenerEntry) {
      listeners.delete(listenerEntry);
    }
  }

  emit<K extends keyof T>(type: K, payload: T[K]): boolean {
    const listeners = this.listeners.get(type);
    if (!listeners) return false;

    listeners.forEach((entry) => {
      entry.wrappedListener(payload);
    });

    return listeners.size > 0;
  }

  removeAllListeners(): void {
    this.listeners.clear();
  }

  async waitFor<K extends keyof T>(
    type: K,
    predicate: (payload: T[K]) => boolean,
    timeoutMs?: number,
  ): Promise<T[K]> {
    return new Promise((resolve, reject) => {
      let timeoutId: ReturnType<typeof setTimeout> | undefined;

      const listener = (payload: T[K]) => {
        if (predicate(payload)) {
          // Clean up
          this.off(type, listener);
          if (timeoutId !== undefined) {
            clearTimeout(timeoutId);
          }
          resolve(payload);
        }
      };

      // Set up timeout if specified
      if (timeoutMs !== undefined) {
        timeoutId = setTimeout(() => {
          this.off(type, listener);
          reject(new Error(`Timeout waiting for event "${String(type)}"`));
        }, timeoutMs);
      }

      this.on(type, listener);
    });
  }

  private debounce<K extends keyof T>(
    func: Listener<T[K]>,
    wait: number,
  ): Listener<T[K]> {
    let timeout: ReturnType<typeof setTimeout>;

    return (detail: T[K]) => {
      if (timeout !== null) {
        clearTimeout(timeout);
      }
      timeout = setTimeout(() => {
        func(detail);
      }, wait);
    };
  }
}
