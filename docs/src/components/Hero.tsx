import Link from "next/link";

export const Hero = () => {
  return (
    <section className="flex items-center justify-center py-16">
      <div className="max-w-full">
        <div className="text-center">
          <h1 className="mt-3 text-[3.5rem] font-bold leading-[4rem] tracking-tight text-slate-400">
            HardhatDB
          </h1>
          <p className="mt-3 text-lg leading-relaxed text-slate-400">
            A MySQL server backed by Badger. Rows are still there after the process exits.
          </p>
        </div>

        <div className="mt-6 flex items-center justify-center gap-4">
          <a
            href="https://github.com/bongani-m/hardhatdb/releases/latest"
            target="_blank"
            rel="noopener noreferrer"
            className="transform rounded-md bg-hardhat-600 px-5 py-3 font-medium text-white transition-colors hover:bg-hardhat-800">
            Download latest
          </a>
          <Link
            href="/demo"
            className="transform rounded-md border bg-hardhat-200 border-slate-200 px-5 py-3 font-medium text-slate-900 transition-colors hover:bg-hardhat-400 hover:text-white">
            See it in action
          </Link>
          <Link
            href="/getting-started"
            className="transform rounded-md border bg-hardhat-200 border-slate-200 px-5 py-3 font-medium text-slate-900 transition-colors hover:bg-hardhat-400 hover:text-white">
            Read the docs
          </Link>
        </div>
      </div>
    </section>
  );
};
