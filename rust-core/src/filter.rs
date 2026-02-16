use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use globset::{Glob, GlobSet, GlobSetBuilder};
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use ignore::Match;

#[derive(Clone, Debug)]
pub struct FilterOptions {
    pub patterns: Vec<String>,
    pub respect_gitignore: bool,
}

#[derive(Debug)]
pub struct MatcherNode {
    parent: Option<Arc<MatcherNode>>,
    gitignore: Option<Gitignore>,
}

impl MatcherNode {
    pub fn root() -> Arc<Self> {
        Arc::new(Self {
            parent: None,
            gitignore: None,
        })
    }
}

#[derive(Debug)]
pub struct FilterEngine {
    root: PathBuf,
    include_set: Option<GlobSet>,
    respect_gitignore: bool,
}

impl FilterEngine {
    pub fn new(root: PathBuf, options: FilterOptions) -> std::io::Result<Self> {
        let include_set = if options.patterns.is_empty() {
            None
        } else {
            let mut builder = GlobSetBuilder::new();
            for pattern in &options.patterns {
                if let Ok(glob) = Glob::new(pattern) {
                    builder.add(glob);
                }
            }
            Some(builder.build().map_err(to_io_error)?)
        };

        Ok(Self {
            root,
            include_set,
            respect_gitignore: options.respect_gitignore,
        })
    }

    pub fn root_matcher(&self) -> Arc<MatcherNode> {
        if !self.respect_gitignore {
            return MatcherNode::root();
        }

        let gitignore_path = self.root.join(".gitignore");
        if !gitignore_path.is_file() {
            return MatcherNode::root();
        }

        let mut builder = GitignoreBuilder::new(&self.root);
        builder.add(gitignore_path);
        let compiled = builder.build().ok();
        Arc::new(MatcherNode {
            parent: None,
            gitignore: compiled,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn matcher_for_dir(
        &self,
        parent: Arc<MatcherNode>,
        dir: &Path,
        cache: &mut HashMap<PathBuf, Arc<MatcherNode>>,
    ) -> Arc<MatcherNode> {
        if !self.respect_gitignore {
            return parent;
        }

        if let Some(hit) = cache.get(dir) {
            return Arc::clone(hit);
        }

        let gitignore_path = dir.join(".gitignore");
        let node = if gitignore_path.is_file() {
            let mut builder = GitignoreBuilder::new(dir);
            builder.add(gitignore_path);
            let compiled = builder.build().ok();
            Arc::new(MatcherNode {
                parent: Some(parent),
                gitignore: compiled,
            })
        } else {
            parent
        };

        cache.insert(dir.to_path_buf(), Arc::clone(&node));
        node
    }

    pub fn is_excluded(&self, path: &Path, is_dir: bool, matcher: &Arc<MatcherNode>) -> bool {
        if self.is_gitignore_ignored(path, is_dir, matcher) {
            return true;
        }

        // globs are treated as a file allow list
        if !is_dir {
            if let Some(include_set) = &self.include_set {
                let rel = path.strip_prefix(&self.root).unwrap_or(path);
                if !include_set.is_match(rel) {
                    return true;
                }
            }
        }

        false
    }

    fn is_gitignore_ignored(&self, path: &Path, is_dir: bool, matcher: &Arc<MatcherNode>) -> bool {
        if !self.respect_gitignore {
            return false;
        }

        let mut chain = Vec::new();
        let mut cursor: Option<Arc<MatcherNode>> = Some(Arc::clone(matcher));
        while let Some(node) = cursor {
            chain.push(Arc::clone(&node));
            cursor = node.parent.as_ref().map(Arc::clone);
        }
        chain.reverse();

        let mut decision: Option<bool> = None;
        for node in chain {
            if let Some(gitignore) = &node.gitignore {
                match gitignore.matched_path_or_any_parents(path, is_dir) {
                    Match::None => {}
                    Match::Ignore(_) => decision = Some(true),
                    Match::Whitelist(_) => decision = Some(false),
                }
            }
        }

        decision.unwrap_or(false)
    }
}

fn to_io_error(err: globset::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, err.to_string())
}
