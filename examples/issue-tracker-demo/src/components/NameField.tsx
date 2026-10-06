/** The display name that this browser writes with. */
export function NameField({ name, onName }: { name: string; onName: (name: string) => void }) {
  return (
    <label className="name-field">
      <span>Name</span>
      <input
        value={name}
        onChange={(event) => onName(event.target.value)}
        placeholder="anonymous"
        aria-label="Your display name"
        maxLength={40}
      />
    </label>
  );
}
