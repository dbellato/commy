import React, { useEffect, useRef, useState } from "react";
import {
  ChatInput as ChatInputFluent,
  ImperativeControlPlugin,
  ImperativeControlPluginRef,
} from "@fluentui-copilot/react-copilot";
import { ChatInputProps } from "./types";

export const ChatInput: React.FC<ChatInputProps> = ({
  onSubmit,
  isGenerating,
  currentUserMessage,
}) => {
  const [inputText, setInputText] = useState<string>("");
  const controlRef = useRef<ImperativeControlPluginRef>(null);

  // --- history settings ---
  const MAX_HISTORY = 20;

  // In-memory history (session-only)
  const historyRef = useRef<string[]>([]);
  const histIdxRef = useRef<number>(-1); // -1 = not browsing
  const draftRef = useRef<string>(""); // draft before ArrowUp browsing
  const isRecallingRef = useRef<boolean>(false); // prevents onChange resetting history while recalling

  // Keep local state + editor in sync if app sets a message externally
  useEffect(() => {
    if (currentUserMessage !== undefined) {
      const v = (currentUserMessage ?? "").toString();
      controlRef.current?.setInputText(v);
      setInputText(v);
      histIdxRef.current = -1;
      draftRef.current = "";
    }
  }, [currentUserMessage]);
  // --- autofocus on mount ---
  useEffect(() => {
    requestAnimationFrame(() => {
      const editor = document.querySelector<HTMLElement>('[data-testid="chat-input"] [contenteditable], [data-testid="chat-input"] textarea, [data-testid="chat-input"] input');
      editor?.focus();
    });
  }, []);

  const clearInput = () => {
    isRecallingRef.current = true;
    setInputText("");
    controlRef.current?.setInputText("");
    // release the guard after React/Fluent propagate change events
    requestAnimationFrame(() => {
      isRecallingRef.current = false;
    });

    histIdxRef.current = -1;
    draftRef.current = "";
  };

  const pushHistory = (text: string) => {
    const t = text.trim();
    if (!t) return;

    const h = historyRef.current;

    // avoid duplicate consecutive entries
    if (h.length === 0 || h[h.length - 1] !== t) {
      h.push(t);
    }

    // keep only last MAX_HISTORY
    if (h.length > MAX_HISTORY) {
      h.splice(0, h.length - MAX_HISTORY);
    }
  };

  const send = (text: string) => {
    const t = (text ?? "").trim();
    if (!t) return;

    pushHistory(t);
    onSubmit(t);
    clearInput();
  };

  const recallTo = (nextValue: string) => {
    isRecallingRef.current = true;

    setInputText(nextValue);
    controlRef.current?.setInputText(nextValue);

    // release after the UI processes the programmatic change
    requestAnimationFrame(() => {
      isRecallingRef.current = false;
    });
  };

  // Up/Down handler on the editor (shell-like history)
  const handleEditorKeyDown = (e: React.KeyboardEvent) => {
    const h = historyRef.current;
    if (h.length === 0) return;

    const target = e.target as HTMLInputElement | HTMLTextAreaElement | null;
    const value = target?.value ?? inputText;

    const selStart = target?.selectionStart ?? 0;
    const selEnd = target?.selectionEnd ?? 0;

    // Only trigger history when caret is at start (Up) or end (Down)
    // so Arrow keys still work normally when editing within the text.
    const atStart = selStart === 0 && selEnd === 0;
    const atEnd = selStart === value.length && selEnd === value.length;

    if (e.key === "ArrowUp" && atStart) {
      e.preventDefault();

      if (histIdxRef.current === -1) {
        draftRef.current = value; // save current draft before browsing
      }

      // go older
      histIdxRef.current = Math.min(h.length - 1, histIdxRef.current + 1);
      const recalled = h[h.length - 1 - histIdxRef.current];
      recallTo(recalled);
      return;
    }

    if (e.key === "ArrowDown" && atEnd) {
      if (histIdxRef.current === -1) return;
      e.preventDefault();

      // go newer
      histIdxRef.current -= 1;

      const recalled =
        histIdxRef.current === -1
          ? draftRef.current // restore draft
          : h[h.length - 1 - histIdxRef.current];

      recallTo(recalled);
    }
  };

  return (
    <ChatInputFluent
      aria-label="Chat Input"
      charactersRemainingMessage={(_value: number) => ``} // required by fluentui-copilot API
      data-testid="chat-input"
      disableSend={isGenerating}
      isSending={isGenerating}
      onChange={(_, d) => {
        setInputText(d.value ?? "");

        // Only reset navigation if user is typing (not when we recalled via arrows)
        if (!isRecallingRef.current) {
          histIdxRef.current = -1;
          draftRef.current = "";
        }
      }}
      onSubmit={(_, d) => {
        send(d.value ?? "");
      }}
      placeholderValue="Scrivi la tua richiesta..."
      editor={{
        onKeyDown: handleEditorKeyDown,
      }}
    >
      <ImperativeControlPlugin ref={controlRef} />
    </ChatInputFluent>
  );
};

export default ChatInput;
