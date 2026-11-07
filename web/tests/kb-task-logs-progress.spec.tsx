import React from "react";
import { render, screen } from "@testing-library/react";
import { beforeEach, expect, it, vi } from "vitest";
import KbTaskLogs from "@/components/knowledge/KbTaskLogs";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

beforeEach(() => {
  Element.prototype.scrollTo = vi.fn();
});

it("shows exact reattached counters without making sub-one-percent progress look done", () => {
  render(<KbTaskLogs kb={{
    name: "large-library",
    status: "processing",
    statistics: {},
    progress: {
      task_id: "upload-1",
      stage: "processing_file",
      message: "Indexed 45 files",
      current: 45,
      total: 20203,
    },
  }} />);

  expect(screen.getByText(/45\/20203/)).toHaveTextContent("Current phase: <1% (45/20203)");
  const bar = screen.getByRole("progressbar");
  expect(bar).toHaveAttribute("aria-valuenow", String((45 / 20203) * 100));
  expect(bar.firstElementChild).toHaveStyle({ width: `${(45 / 20203) * 100}%` });
});
