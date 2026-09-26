---
title: "Contributing to Felix"
---

How to get a change into Felix: what to build, what the checks demand, and what a good PR here looks like.

## Code of Conduct

Felix is an open and welcoming project. We expect all contributors to:

- Be respectful and considerate
- Focus on constructive feedback
- Help create a positive community
- Report unacceptable behavior to project maintainers

## Ways to Contribute

### Reporting Bugs

Found a bug? Please open an issue with:

1. **Clear title**: Summarize the issue
2. **Description**: What happened vs what you expected
3. **Reproduction steps**: Minimal steps to reproduce
4. **Environment**: OS, Rust version, Felix version
5. **Logs/errors**: Relevant error messages or stack traces

**Template**:
```markdown
**Bug Description**
A clear description of the bug.

**To Reproduce**
1. Start broker with config X
2. Run client command Y
3. Observe error Z

**Expected Behavior**
What should have happened.

**Environment**
- OS: Ubuntu 22.04
- Rust: 1.97.1
- Felix: main branch, commit abc123

**Logs**
```
Paste relevant logs here
```
```

### Suggesting Features

Have an idea? Open an issue with:

1. **Problem statement**: What problem does this solve?
2. **Proposed solution**: How would it work?
3. **Alternatives considered**: Other approaches you've thought about
4. **Additional context**: Use cases, examples

**Template**:
```markdown
**Problem**
Describe the problem or limitation.

**Proposed Solution**
How this feature would work.

**Alternatives**
Other solutions considered and why they're less ideal.

**Use Case**
Real-world scenario where this would help.
```

### Improving Documentation

Documentation is always welcome! You can:

- Fix typos or clarify existing docs
- Add examples or tutorials
- Improve API documentation
- Write guides for common scenarios

See [Building the Docs](#building-documentation) below.

### Contributing Code

See [Development Workflow](#development-workflow) for details.

## Getting Started

### Prerequisites

- **Rust 1.97.1+**: Install via [rustup](https://rustup.rs/)
- **Git**: For version control
- **Task** (optional): Install from [taskfile.dev](https://taskfile.dev/)
- **Development tools**: `cargo-fmt`, `cargo-clippy`

### Fork and Clone

```bash
# Fork repository on GitHub
# Then clone your fork
git clone https://github.com/YOUR_USERNAME/felix.git
cd felix

# Add upstream remote
git remote add upstream https://github.com/gabloe/felix.git
```

### Build and Test

```bash
# Build everything
cargo build --workspace

# Run tests
cargo test --workspace

# Or use Task
task build
task test
```

## Development Workflow

### 1. Create a Branch

```bash
# Sync with upstream
git fetch upstream
git checkout main
git merge upstream/main

# Create feature branch
git checkout -b feature/my-feature

# Or bugfix branch
git checkout -b fix/issue-123
```

**Branch naming**:
- `feature/description`: New features
- `fix/description`: Bug fixes
- `docs/description`: Documentation changes
- `refactor/description`: Code refactoring
- `test/description`: Test additions/improvements

### 2. Make Changes

**Code style**:

Felix follows standard Rust style guidelines:

```bash
# Format code
cargo fmt --all

# Or with Task
task fmt
```

**Run linter**:

```bash
# Check formatting
cargo fmt -- --check

# Run clippy
cargo clippy --workspace --all-targets --all-features -- -D warnings

# Or with Task
task lint
```

**Writing tests**:

All new code should include tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_my_feature() {
        // Arrange
        let input = setup_test_data();
        
        // Act
        let result = my_function(input);
        
        // Assert
        assert_eq!(result, expected_value);
    }
}
```

**Test async code**:

```rust
#[tokio::test]
async fn test_async_feature() {
    let result = my_async_function().await;
    assert!(result.is_ok());
}
```

### 3. Commit Changes

**Commit message format**:

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types**:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or modifying tests
- `chore`: Build process or tooling changes

**Example**:

```bash
git add .
git commit -m "feat(broker): add configurable batch timeout

Adds FELIX_EVENT_BATCH_MAX_DELAY_US configuration to control
the maximum delay before flushing event batches. This allows
users to tune the latency vs throughput trade-off.

Closes #123"
```

**Guidelines**:
- Use present tense ("add" not "added")
- Keep subject line under 72 characters
- Reference issues in footer (`Closes #123`, `Fixes #456`)
- Add breaking changes in footer: `BREAKING CHANGE: description`

### 4. Push and Create PR

```bash
# Push to your fork
git push origin feature/my-feature

# Create pull request on GitHub
```

**Pull request template**:

```markdown
## Description
Brief description of changes.

## Motivation
Why is this change needed?

## Changes
- List of changes made

## Testing
How was this tested?

## Checklist
- [ ] Tests added/updated
- [ ] Documentation updated
- [ ] Ran `task lint`
- [ ] Ran `task test`
- [ ] No breaking changes (or documented in commit)
```

### 5. Code Review

- Respond to feedback promptly
- Make requested changes in new commits
- Push updates to the same branch
- Request re-review when ready

### 6. Merge

Once approved:
- Maintainers will merge your PR
- Delete your feature branch after merge

```bash
git checkout main
git pull upstream main
git branch -d feature/my-feature
```

## Code Style Guidelines

### Rust Style

**Follow standard conventions**:

```rust
// Use descriptive names
fn calculate_batch_size(events: &[Event]) -> usize { ... }

// Document public APIs
/// Publishes an event to the specified stream.
///
/// # Arguments
/// * `stream` - The target stream name
/// * `payload` - The event payload
///
/// # Returns
/// Result indicating success or error
pub async fn publish(&self, stream: &str, payload: &[u8]) -> Result<()> { ... }

// Use Result for errors
fn parse_config(path: &str) -> Result<Config> { ... }

// Prefer ? operator over unwrap()
let config = parse_config(path)?;

// Use meaningful error messages
return Err(anyhow!("Failed to bind to {}: {}", addr, err));
```

### Formatting

```bash
# Format all code
cargo fmt --all

# Check formatting in CI
cargo fmt -- --check
```

### Linting

```bash
# Run clippy
cargo clippy --workspace --all-targets --all-features -- -D warnings

# Fix automatically where possible
cargo clippy --fix
```

### Comments

**When to comment**:
- Complex algorithms
- Non-obvious design decisions
- Performance-critical sections
- Workarounds for external issues

**When not to comment**:
- Obvious code
- What the code does (code should be self-documenting)

```rust
// Good: Explains why
// We batch events to amortize framing overhead across multiple messages.
// Empirical testing shows 64 events per batch optimizes for 1-4KB payloads.
const DEFAULT_BATCH_SIZE: usize = 64;

// Bad: Restates the obvious
// Set the batch size to 64
const DEFAULT_BATCH_SIZE: usize = 64;
```

### Error Handling

```rust
// Use anyhow for application code
use anyhow::{Context, Result};

fn load_config(path: &str) -> Result<Config> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("Failed to read config from {}", path))?;
    let config: Config = serde_yaml_ng::from_str(&contents)
        .context("Failed to parse config YAML")?;
    Ok(config)
}

// Use custom error types for libraries
#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    #[error("Connection closed")]
    ConnectionClosed,
    #[error("Invalid frame: {0}")]
    InvalidFrame(String),
}
```

## Testing Guidelines

### Test Coverage

- All public APIs must have tests
- Bug fixes must include regression tests
- Aim for >80% code coverage

### Test Organization

```rust
#[cfg(test)]
mod tests {
    use super::*;

    // Unit tests for internal functions
    #[test]
    fn test_parse_frame() { ... }

    // Integration tests for APIs
    #[tokio::test]
    async fn test_publish_subscribe_flow() { ... }
}
```

### Running Tests

```bash
# All tests
cargo test --workspace

# Specific test
cargo test test_name

# With output
cargo test -- --nocapture

# With coverage
task coverage
```

### Test Fixtures

```rust
// Create reusable test helpers
fn create_test_broker() -> Broker {
    BrokerBuilder::new()
        .with_config(test_config())
        .build()
        .unwrap()
}

#[tokio::test]
async fn test_feature() {
    let broker = create_test_broker();
    // Test code
}
```

## Documentation

### Code Documentation

```rust
/// A brief one-line summary.
///
/// More detailed description if needed. Can span multiple paragraphs.
///
/// # Arguments
/// * `arg1` - Description of arg1
/// * `arg2` - Description of arg2
///
/// # Returns
/// Description of return value
///
/// # Errors
/// Description of error conditions
///
/// # Examples
/// ```
/// use felix_broker::Broker;
///
/// let broker = Broker::new();
/// broker.start().await?;
/// ```
pub async fn my_function(arg1: Type1, arg2: Type2) -> Result<Type3> {
    // Implementation
}
```

### Building Documentation

**API docs**:

```bash
# Build and open docs
cargo doc --open --no-deps

# Build all docs
cargo doc --workspace
```

**User documentation**:

```bash
# Install dependencies and serve locally
cd docs-site
npm install
npm run dev

# Build static site
npm run build
```

## Performance Considerations

### Benchmarking

```bash
# Run latency benchmarks
cargo run --release -p felix-broker-service --features demo --bin latency-demo -- \
    --binary --fanout 10 --batch 64 --payload 4096

# Run cache benchmarks
cargo run --release -p felix-broker-service --features demo --bin cache-demo
```

### Profiling

**CPU profiling**:

```bash
# Linux perf
sudo perf record -g cargo run --release -p felix-broker-service
sudo perf report

# Flamegraph
cargo install flamegraph
cargo flamegraph -p felix-broker-service
```

**Memory profiling**:

```bash
# Valgrind (debug build)
valgrind --leak-check=full ./target/debug/broker

# Heaptrack (Linux)
heaptrack cargo run --release -p felix-broker-service
```

## Release Process

(For maintainers)

1. Update version in `Cargo.toml`
2. Update `CHANGELOG.md`
3. Create git tag: `git tag -a v0.1.0 -m "Release v0.1.0"`
4. Push tag: `git push origin v0.1.0`
5. GitHub Actions builds and publishes release

## Getting Help

**Stuck?**

- **GitHub Discussions**: Ask questions
- **GitHub Issues**: Bug reports and feature requests
- **Documentation**: [docs-site/](https://gabloe.github.io/felix/)

**Before asking**:

1. Check existing issues/discussions
2. Review documentation
3. Try to create a minimal reproduction

## Recognition

Contributors are recognized in:

- `CONTRIBUTORS.md` (added on first merged PR)
- Release notes
- GitHub contributors graph

Thank you for contributing to Felix! 🎉

## Next Steps

- **Build system**: [Building & Testing Guide](/felix/development/building/)
- **Project structure**: [Project Structure](/felix/development/project-structure/)
- **Architecture**: [System Design](/felix/architecture/system-design/)
